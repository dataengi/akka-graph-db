package com.dataengi.graphDb.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorSystem, Behavior, PostStop}
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.cluster.sharding.typed.{ClusterShardingSettings, ShardedDaemonProcessSettings}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{Offset, PersistenceQuery, TimeBasedUUID}
import akka.persistence.typed.PersistenceId
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import akka.stream.scaladsl.{RestartSource, Sink, Source}
import akka.stream.{KillSwitches, SharedKillSwitch}
import akka.{Done, NotUsed}
import com.dataengi.graphDb.models.Event
import com.dataengi.graphDb.readmodel.EventProcessorSettings
import com.datastax.oss.driver.api.core.cql.Row
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object EventProcessor {

  def init(system: ActorSystem[_],
           settings: EventProcessorSettings,
           eventProcessorStream: String => EventProcessorStream): Unit = {
    val shardedDaemonSettings = ShardedDaemonProcessSettings(system)
      .withKeepAliveInterval(settings.keepAliveInterval)
      .withShardingSettings(
        ClusterShardingSettings(system).withRole("read-model")
      )
    ShardedDaemonProcess(system).init[Nothing](
      s"event-processors-${settings.id}",
      settings.parallelism,
      i => EventProcessor(eventProcessorStream(s"${settings.tagPrefix}-$i")),
      shardedDaemonSettings,
      None
    )
  }

  def apply(eventProcessorStream: EventProcessorStream): Behavior[Nothing] = {
    Behaviors.setup[Nothing] { ctx =>
      val killSwitch = KillSwitches.shared("eventProcessorSwitch")
      eventProcessorStream.runQueryStream(killSwitch)
      Behaviors.receiveSignal[Nothing] {
        case (_, PostStop) =>
          killSwitch.shutdown()
          Behaviors.same
      }
    }
  }
}

abstract class EventProcessorStream(system: ActorSystem[_],
                                    executionContext: ExecutionContext,
                                    eventProcessorId: String,
                                    tag: String) {

  protected val log: Logger = LoggerFactory.getLogger(getClass)
  implicit val sys: ActorSystem[_] = system
  implicit val ec: ExecutionContext = executionContext

  private val query =
    PersistenceQuery(system.toClassic)
      .readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
  private val session =
    CassandraSessionRegistry(system).sessionFor("alpakka.cassandra")

  protected def processEvent(event: Event,
                             persistenceId: PersistenceId,
                             sequenceNr: Long): Future[Done]

  def runQueryStream(killSwitch: SharedKillSwitch): Unit = {
    RestartSource
      .withBackoff(
        minBackoff = 500.millis,
        maxBackoff = 20.seconds,
        randomFactor = 0.1
      ) { () =>
        Source.futureSource {
          readOffset().map { offset =>
            log.info(
              "Starting stream for tag [{}] from offset [{}]",
              tag,
              offset
            )
            processEventsByTag(offset)
            // groupedWithin can be used here to improve performance by reducing number of offset writes,
            // with the trade-off of possibility of more duplicate events when stream is restarted
              .mapAsync(1)(writeOffset)
          }
        }
      }
      .via(killSwitch.flow)
      .runWith(Sink.ignore)
  }

  private def processEventsByTag(offset: Offset): Source[Offset, NotUsed] = {
    query.eventsByTag(tag, offset).mapAsync(1) { eventEnvelope =>
      eventEnvelope.event match {
        case event: Event =>
          processEvent(
            event,
            PersistenceId.ofUniqueId(eventEnvelope.persistenceId),
            eventEnvelope.sequenceNr
          ).map(_ => eventEnvelope.offset)
        case other =>
          Future.failed(
            new IllegalArgumentException(
              s"Unexpected event [${other.getClass.getName}]"
            )
          )
      }
    }
  }

  private def readOffset(): Future[Offset] = {
    session
      .selectOne(
        "SELECT timeUuidOffset FROM read_model.offsetStore WHERE eventProcessorId = ? AND tag = ?",
        eventProcessorId,
        tag
      )
      .map(extractOffset)
  }

  private def extractOffset(maybeRow: Option[Row]): Offset = {
    maybeRow match {
      case Some(row) =>
        val uuid = row.getUuid("timeUuidOffset")
        if (uuid == null) {
          startOffset()
        } else {
          TimeBasedUUID(uuid)
        }
      case None => startOffset()
    }
  }

  // start looking from one week back if no offset was stored
  private def startOffset(): Offset = {
    query.timeBasedUUIDFrom(
      System.currentTimeMillis() - (7 * 24 * 60 * 60 * 1000)
    )
  }

  private def writeOffset(offset: Offset): Future[Done] = {
    offset match {
      case t: TimeBasedUUID =>
        session.executeWrite(
          "INSERT INTO read_model.offsetStore (eventProcessorId, tag, timeUuidOffset) VALUES (?, ?, ?)",
          eventProcessorId,
          tag,
          t.value
        )

      case _ =>
        throw new IllegalArgumentException(s"Unexpected offset type $offset")
    }

  }

}
