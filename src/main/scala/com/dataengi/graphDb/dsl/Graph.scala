package com.dataengi.graphDb.dsl

import com.dataengi.graphDb.actors.Node.{NodeId, NodeState}
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

object Graph {

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[StringValue], name = "string"),
      new JsonSubTypes.Type(value = classOf[BoolValue], name = "boolean"),
      new JsonSubTypes.Type(value = classOf[NumberValue], name = "number"),
      new JsonSubTypes.Type(value = classOf[EmptyValue], name = "empty"),
      new JsonSubTypes.Type(value = classOf[Field], name = "field"),
      new JsonSubTypes.Type(value = classOf[ListValue], name = "list")
    )
  )
  sealed trait Value {
    def apply(graphNode: NodeState): Value = this
  }

  case class NodeRef(nodeId: NodeId)

  sealed case class GraphNodes(nodes: Set[NodeState]) {
    def nodeIds: Set[NodeId] = nodes.map(_.nodeId)
  }

  case class EmptyValue() extends Value

  case class Field(name: String) extends Value {
    override def apply(graphNode: NodeState): Value = {
      if (graphNode.attributes.contains(name))
        graphNode.attributes(name)
      else EmptyValue()
    }
  }

  case class BoolValue(value: Boolean) extends Value

  case class NumberValue(value: Double) extends Value

  case class StringValue(value: String) extends Value

  case class ListValue(values: Set[Value]) extends Value

}
