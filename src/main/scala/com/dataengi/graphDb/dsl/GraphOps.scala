package com.dataengi.graphDb.dsl

import com.dataengi.graphDb.actors.Node.{NodeId, NodeState, NodeType, RelationType}

trait GraphOps {
  import Graph._

  sealed trait Predicate {
    def apply(graphNode: NodeState): Boolean
  }

  case class Func(func: NodeState => Boolean) extends Predicate {
    override def apply(graphNode: NodeState): Boolean = func(graphNode)
  }

  case class Or(x: Predicate, y: Predicate) extends Predicate {
    override def apply(graphNode: NodeState): Boolean =
      x(graphNode) || y(graphNode)
  }

  case class And(x: Predicate, y: Predicate) extends Predicate {
    override def apply(graphNode: NodeState): Boolean =
      x(graphNode) && y(graphNode)
  }

  case class Not(x: Predicate) extends Predicate {
    override def apply(graphNode: NodeState): Boolean = !x(graphNode)
  }

  case class Eq(x: Value, y: Value) extends Predicate {
    override def apply(graphNode: NodeState): Boolean =
      x(graphNode) == y(graphNode)
  }

  case class HasNodeType(`type`: NodeType) extends Predicate {
    override def apply(graphNode: NodeState): Boolean =
      graphNode.`type` == `type`
  }

  case class HasRelation(relationType: RelationType, relationId: NodeId) extends Predicate {
    override def apply(graphNode: NodeState): Boolean =
      graphNode.relations.get(relationId).contains(relationType)
  }

  case class HasRelationId(relationId: NodeId) extends Predicate {
    override def apply(graphNode: NodeState): Boolean =
      graphNode.relations.contains(relationId)
  }

  case class HasRelationType(relationType: RelationType) extends Predicate {
    override def apply(graphNode: NodeState): Boolean =
      graphNode.relations.exists {
        case (_: NodeId, tp: NodeType) => tp == relationType
      }
  }

  implicit class PredicateOps(predicate: Predicate) {
    def and(pred: Predicate): And = And(predicate, pred)
  }

}
