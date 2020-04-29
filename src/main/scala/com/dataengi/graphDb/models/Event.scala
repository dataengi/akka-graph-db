package com.dataengi.graphDb.models

import com.dataengi.graphDb.actors.Node.{FieldType, NodeId, NodeType, RelationType}
import com.dataengi.graphDb.dsl.Graph._

sealed trait Event extends CborSerializable

final case class RelationUpdated(id: NodeId, `type`: RelationType, toId: NodeId)
    extends Event

final case class RelationRemoved(id: NodeId, toId: NodeId) extends Event

final case class Created(id: NodeId,
                         entityType: NodeType,
                         fields: Map[FieldType, Value])
    extends Event

final case class FieldsAdded(id: NodeId, fields: Map[FieldType, Value])
    extends Event

final case class FieldAdded(id: NodeId, name: String, value: Value)
    extends Event

final case class FieldRemoved(id: NodeId, field: String) extends Event
