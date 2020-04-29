package com.dataengi.graphDb.models

import akka.actor.typed.ActorRef
import com.dataengi.graphDb.actors.Node.{FieldType, NodeId, NodeType, RelationType}
import com.dataengi.graphDb.dsl.Graph._
trait Command extends CborSerializable

final case class Create(id: NodeId,
                        name: NodeType,
                        fields: Map[FieldType, Value],
                        replyTo: ActorRef[Reply])
    extends Command

final case class AddField(name: String, value: Value, replyTo: ActorRef[Reply])
    extends Command

final case class AddFields(fields: Map[String, Value], replyTo: ActorRef[Reply])
    extends Command

final case class RemoveField(field: String, replyTo: ActorRef[Reply])
    extends Command

final case class Get(replyTo: ActorRef[Reply]) extends Command

final case class UpdateRelation(`type`: RelationType,
                                toId: NodeId,
                                replyTo: ActorRef[Reply])
    extends Command

final case class RemoveRelation(nodeId: NodeId,
                                toId: NodeId,
                                replyTo: ActorRef[Reply])
    extends Command
