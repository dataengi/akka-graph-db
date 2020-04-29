package com.dataengi.graphDb.readmodel

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import akka.stream.scaladsl.Source
import com.dataengi.graphDb.actors.Node._
import com.dataengi.graphDb.dsl.Graph._
import com.dataengi.graphDb.dsl.ValueInstances._
import com.datastax.oss.driver.api.core.cql.Row

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class ReadModelDAO(implicit private val system: ActorSystem[Nothing]) {
  private implicit val ec: ExecutionContext = system.executionContext
  private val session =
    CassandraSessionRegistry(system).sessionFor("alpakka.cassandra")

  private def rowToNodeState(row: Row): NodeState =
    NodeState(
      nodeId = row.getString("nodeId"),
      `type` = row.getString("type"),
      attributes = row
        .getMap("attributes", classOf[FieldType], classOf[String])
        .asScala
        .view
        .mapValues(stringToValue)
        .toMap,
      relations = row
        .getMap("relations", classOf[String], classOf[String])
        .asScala
        .toMap,
    )

  def getNode(nodeId: NodeId): Future[Option[NodeState]] =
    session
      .selectOne("SELECT * FROM read_model.nodes WHERE nodeId = ?", nodeId)
      .map(opt => opt map rowToNodeState)

  def getByIds(nodeIds: Set[NodeId]): Future[Set[NodeState]] =
    session
      .selectAll(
        "SELECT * FROM read_model.nodes WHERE nodeId IN (%s)"
          .format(nodeIds.mkString("'", "', '", "'"))
      )
      .map(_.map(rowToNodeState).toSet)

  def getAllNodes: Source[NodeState, NotUsed] =
    session
      .select("SELECT * FROM read_model.nodes")
      .map(rowToNodeState)

  def getAllWithRelation(
      relationType: RelationType): Source[NodeState, NotUsed] =
    session
      .select(
        "SELECT * FROM read_model.nodes WHERE relations CONTAINS KEY ? ALLOW FILTERING",
        relationType
      )
      .map(rowToNodeState)

  def getByType(`type`: NodeType): Source[NodeState, NotUsed] =
    session
      .select(
        "SELECT * FROM read_model.nodes WHERE type = ? ALLOW FILTERING",
        `type`
      )
      .map(rowToNodeState)

  def getByNodeTypes(types: Set[NodeType]): Source[NodeState, NotUsed] =
    session
      .select(
        "SELECT * FROM read_model.nodes WHERE type IN (%s) ALLOW FILTERING".format(types.mkString("'", "', '", "'"))
      )
      .map(rowToNodeState)

  def getByTypeAndField(`type`: NodeType,
                        fieldName: FieldType,
                        fieldValue: Value): Source[NodeState, NotUsed] =
    session
      .select(
        "SELECT * FROM read_model.nodes WHERE type = ? AND attributes[?] = ? ALLOW FILTERING",
        `type`,
        fieldName,
        valueToString(fieldValue)
      )
      .map(rowToNodeState)

  def getByTypeAndNotField(`type`: NodeType,
                           fieldName: FieldType,
                           fieldValue: Value): Source[NodeState, NotUsed] =
    session
      .select(
        "SELECT * FROM read_model.nodes WHERE type = ? AND attributes[?] <> ? ALLOW FILTERING",
        `type`,
        fieldName,
        valueToString(fieldValue)
      )
      .map(rowToNodeState)

  def getByTypeAndRelationType(
      `type`: NodeType,
      relationType: RelationType
  ): Source[NodeState, NotUsed] =
    session
      .select(
        "SELECT * FROM read_model.nodes WHERE type = ? AND relations CONTAINS KEY ? ALLOW FILTERING",
        `type`,
        relationType
      )
      .map(rowToNodeState)

  def getByTypeAndFieldAndRelation(
      `type`: NodeType,
      fieldName: FieldType,
      fieldValue: Value,
      relationType: RelationType,
      relationId: NodeId): Source[NodeState, NotUsed] =
    session
      .select(
        "SELECT * FROM read_model.nodes WHERE type = ? AND attributes[?] = ? AND relations[?] = ? ALLOW FILTERING",
        `type`,
        fieldName,
        valueToString(fieldValue),
        relationType,
        relationId
      )
      .map(rowToNodeState)

  def getByTypeAndRelation(`type`: NodeType,
                           relationType: RelationType,
                           relationId: NodeId): Source[NodeState, NotUsed] =
    session
      .select(
        "SELECT * FROM read_model.nodes WHERE type = ? AND attributes[?] = ? AND relations[?] = ? ALLOW FILTERING",
        `type`,
        relationType,
        relationId
      )
      .map(rowToNodeState)

}
