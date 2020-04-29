package com.dataengi.graphDb.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import com.dataengi.graphDb.dsl.Graph._
import com.dataengi.graphDb.models._
import com.dataengi.graphDb.readmodel._

object Node {

  type NodeId = String
  type NodeType = String
  type FieldType = String
  type Fields = Map[FieldType, Value]
  type RelationType = String

  final case class NodeState(nodeId: NodeId,
                             `type`: NodeType,
                             attributes: Fields = Map(),
                             relations: Map[NodeId, RelationType] = Map())

  def empty: NodeState = NodeState(null, null)

  val EntityKey: EntityTypeKey[Command] = EntityTypeKey[Command]("Node")

  def init(system: ActorSystem[_],
           eventProcessorSettings: EventProcessorSettings): Unit = {
    ClusterSharding(system).init(Entity(EntityKey) { entityContext =>
      val n = math.abs(
        entityContext.entityId.hashCode % eventProcessorSettings.parallelism
      )
      val eventProcessorTag = eventProcessorSettings.tagPrefix + "-" + n
      Node(entityContext.entityId, Set(eventProcessorTag))
    }.withRole("write-model"))
  }

  def apply(entityId: String,
            eventProcessorTags: Set[String]): Behavior[Command] = {
    Behaviors.setup { ctx =>
      EventSourcedBehavior[Command, Event, NodeState](
        PersistenceId("Node", entityId),
        empty,
        (state, command) => commandHandler(ctx, state, command),
        (state, event) => eventHandler(ctx, state, event)
      ).withTagger(_ => eventProcessorTags)
    }
  }

  def commandHandler(ctx: ActorContext[Command],
                     state: NodeState,
                     command: Command): Effect[Event, NodeState] = {
    def ifStateNotEmpty(
      replyTo: ActorRef[Reply]
    )(effect: => Effect[Event, NodeState]) =
      if (state.nodeId != null)
        effect
      else
        Effect.none.thenReply(replyTo)((_: NodeState) => EmptyNodeReply)

    command match {

      case Create(id, name, fields, replyTo) =>
        if (state.nodeId == null)
          Effect
            .persist(Created(id, name, fields))
            .thenRun(node => replyTo ! SuccessReply(node))
        else
          Effect.reply(replyTo)(NodeIsAlreadyCreated)

      case AddFields(fields, replyTo) =>
        ifStateNotEmpty(replyTo) {
          Effect
            .persist(FieldsAdded(state.nodeId, fields))
            .thenReply(replyTo)(entity => SuccessReply(entity))
        }

      case AddField(name, value, replyTo) =>
        ifStateNotEmpty(replyTo) {
          Effect
            .persist(FieldAdded(state.nodeId, name, value))
            .thenReply(replyTo)(entity => SuccessReply(entity))
        }

      case UpdateRelation(relationType, toId, replyTo) =>
        ifStateNotEmpty(replyTo) {
          Effect
            .persist(RelationUpdated(state.nodeId, relationType, toId))
            .thenReply(replyTo)(entity => SuccessReply(entity))
        }

      case RemoveField(field, replyTo) =>
        ifStateNotEmpty(replyTo) {
          Effect
            .persist(FieldRemoved(state.nodeId, field))
            .thenReply(replyTo)(entity => SuccessReply(entity))
        }

      case RemoveRelation(nodeId, toId, replyTo) =>
        ifStateNotEmpty(replyTo) {
          Effect
            .persist(RelationRemoved(nodeId, toId))
            .thenReply(replyTo)(entity => SuccessReply(entity))
        }

      case Get(replyTo) =>
        ifStateNotEmpty(replyTo) {
          Effect.reply(replyTo)(SuccessReply(state))
        }

      case _ =>
        Effect.none
    }
  }

  def eventHandler(ctx: ActorContext[Command],
                   state: NodeState,
                   event: Event): NodeState = event match {

    case Created(id, entityType, fields) =>
      state.copy(nodeId = id, `type` = entityType, attributes = fields)

    case FieldsAdded(_, fields) =>
      state.copy(attributes = state.attributes ++ fields)

    case FieldAdded(_, name, value) =>
      state.copy(attributes = state.attributes + (name -> value))

    case RelationUpdated(_, relationType, toId) =>
      state.copy(relations = state.relations + (toId -> relationType))

    case RelationRemoved(_, toId) =>
      state.copy(relations = state.relations - toId)

    case FieldRemoved(_, field) =>
      state.copy(attributes = state.attributes - field)

    case _ => state
  }

  val EmptyNodeReply: ReplyError = ReplyError("Node is not created!")
  val NodeIsAlreadyCreated: ReplyError = ReplyError("Node is already created!")
  val NodeNotCreatedReply: ReplyError = ReplyError("Node is not created!")

}
