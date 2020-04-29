package com.dataengi.graphDb.dsl

import com.dataengi.graphDb.actors.Node.{NodeId, NodeState, NodeType, RelationType}
import com.dataengi.graphDb.dsl.Graph.Field

import scala.concurrent.Future

/*
 * Optimizes execution using supported read model calls
 * */

trait QueryOptimizer {
  def filterByPredicate(predicate: Predicate): Future[Set[NodeState]]
}

class GraphContextQueryOptimizer(implicit gc: GraphContext) extends QueryOptimizer {

  private val optimizer: PartialFunction[Predicate, Future[Set[NodeState]]] = {

    case HasNodeType(nodeType) =>
      gc.filterByNodeType(nodeType)

    case And(And(HasNodeType(nodeType), pred1), pred2) if optimizerWithType(nodeType).isDefinedAt(pred1 and pred2) =>
      optimizerWithType(nodeType)(pred1 and pred2)

    case And(And(pred1, HasNodeType(nodeType)), pred2) if optimizerWithType(nodeType).isDefinedAt(pred1 and pred2) =>
      optimizerWithType(nodeType)(pred1 and pred2)

    case And(pred1, And(HasNodeType(nodeType), pred2)) if optimizerWithType(nodeType).isDefinedAt(pred1 and pred2) =>
      optimizerWithType(nodeType)(pred1 and pred2)

    case And(pred1, And(pred2, HasNodeType(nodeType))) if optimizerWithType(nodeType).isDefinedAt(pred1 and pred2) =>
      optimizerWithType(nodeType)(pred1 and pred2)

    case And(HasNodeType(nodeType), pred) if optimizerWithType(nodeType).isDefinedAt(pred) =>
      optimizerWithType(nodeType)(pred)

    case And(pred, HasNodeType(nodeType)) if optimizerWithType(nodeType).isDefinedAt(pred) =>
      optimizerWithType(nodeType)(pred)

    case Or(HasNodeType(tp1), HasNodeType(tp2)) =>
      gc.filterByNodeTypes(Set(tp1, tp2))
  }

  private def optimizerWithType(nodeType: NodeType): PartialFunction[Predicate, Future[Set[NodeState]]] = {
    case Eq(field @ Field(_), value) =>
      gc.filter(nodeType, field, value)

    case Eq(value, field @ Field(_)) =>
      gc.filter(nodeType, field, value)

    case Not(Eq(field @ Field(_), value)) =>
      gc.filterNotField(nodeType, field, value)

    case Not(Eq(value, field @ Field(_))) =>
      gc.filterNotField(nodeType, field, value)

    case relation @ HasRelation(_: RelationType, _: NodeId) =>
      gc.filter(nodeType, relation)

    case HasRelationType(relationType: RelationType) =>
      gc.filter(nodeType, relationType)

    case And(relation @ HasRelation(_: RelationType, _: NodeId), Eq(field @ Field(_), value)) =>
      gc.filter(nodeType, relation, field, value)

  }

  override def filterByPredicate(predicate: Predicate): Future[Set[NodeState]] =
    if (optimizer.isDefinedAt(predicate)) optimizer(predicate)
    else gc.filter(predicate.apply _)

}

class QueryOptimizerDefault(implicit gc: GraphContext) extends QueryOptimizer {
  override def filterByPredicate(predicate: Predicate): Future[Set[NodeState]] =
    gc.filter(predicate.apply _)
}
