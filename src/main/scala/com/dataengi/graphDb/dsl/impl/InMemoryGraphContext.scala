package com.dataengi.graphDb.dsl.impl

import com.dataengi.graphDb.dsl._
import com.dataengi.graphDb.actors.Node.{
  NodeId,
  NodeState,
  NodeType,
  RelationType
}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

case class InMemoryGraphContext(
    private val map: mutable.Map[NodeId, NodeState] = mutable.Map.empty)(
    implicit ex: ExecutionContext)
    extends GraphContext {

  private val DefaultRelationType = "default"

  override def filter(func: NodeState => Boolean): Future[Set[NodeState]] =
    Future.successful(
      map
        .filter {
          case (_, node) => func(node)
        }
        .values
        .toSet)

  override def createNode(id: NodeId, `type`: NodeType): Future[Unit] =
    Future.successful(map += id -> NodeState(id, `type`))

  override def removeAttr(id: NodeId, name: String): Future[Unit] = {
    for { node <- nodeById(id) } yield {
      map(id) = node.copy(attributes = node.attributes - name)
    }
  }

  override def addRelation(id: NodeId, toId: NodeId): Future[Unit] = {
    for { node <- nodeById(id) } yield {
      map(id) =
        node.copy(relations = node.relations + (toId -> DefaultRelationType))
    }
  }

  override def addRelation(id: NodeId,
                           relationType: RelationType,
                           toId: NodeId): Future[Unit] = {
    for { node <- nodeById(id) } yield {
      map(id) = node.copy(relations = node.relations + (toId -> relationType))
    }
  }

  override def nodeById(id: NodeId): Future[NodeState] =
    Future.successful(map(id))

  override def nodeByIds(ids: Set[NodeId]): Future[Set[NodeState]] =
    Future.successful(ids.flatMap(id => map.get(id)))

  override def removeRelation(id: NodeId, toId: NodeId): Future[Unit] = {
    for { node <- nodeById(id) } yield {
      map(id) = node.copy(relations = node.relations - toId)
    }
  }

  override def getAllNodes: Future[Set[NodeState]] =
    Future.successful(map.values.toSet)

  override def filter(nodeType: NodeType,
                      relationType: RelationType): Future[Set[NodeState]] =
    Future.successful(
      map
        .filter {
          case (_, node) =>
            node.`type` == nodeType &&
              node.relations.exists(_._2 == relationType)
        }
        .values
        .toSet
    )

  override def filter(nodeType: NodeType,
                      field: Graph.Field,
                      fieldValue: Graph.Value): Future[Set[NodeState]] =
    Future.successful(
      map
        .filter {
          case (_, node) =>
            (node.`type` == nodeType) &&
              node.attributes.get(field.name).contains(fieldValue)
        }
        .values
        .toSet
    )

  override def addAttr(id: NodeId,
                       name: String,
                       value: Graph.Value): Future[Unit] =
    addAttrs(id, Set(name -> value))

  override def addAttrs(id: NodeId,
                        attrs: Set[(String, Graph.Value)]): Future[Unit] = {
    for { node <- nodeById(id) } yield {
      map(id) = node.copy(attributes = node.attributes ++ attrs)
    }
  }

  override def filter(nodeType: NodeType,
                      relation: HasRelation,
                      field: Graph.Field,
                      fieldValue: Graph.Value): Future[Set[NodeState]] =
    Future.successful(
      map
        .filter {
          case (_, node) =>
            (node.`type` == nodeType) &&
              node.relations
                .get(relation.relationId)
                .contains(relation.relationType) &&
              node.attributes.get(field.name).contains(fieldValue)
        }
        .values
        .toSet
    )

  override def filter(nodeId: NodeId): Future[Set[NodeState]] =
    Future.successful(Set(map(nodeId)))

  override def filterByNodeType(nodeType: NodeType): Future[Set[NodeState]] =
    Future.successful(
      map.filter { case (_, node) => node.`type` == nodeType }.values.toSet
    )

  override def filter(nodeType: NodeType,
                      relation: HasRelation): Future[Set[NodeState]] =
    Future.successful(
      map
        .filter {
          case (_, node) =>
            (node.`type` == nodeType) && node.relations
              .get(relation.relationId)
              .contains(relation.relationType)
        }
        .values
        .toSet
    )

  override def filterNotField(nodeType: NodeType,
                              field: Graph.Field,
                              fieldValue: Graph.Value): Future[Set[NodeState]] =
    Future.successful(
      map
        .filter {
          case (_, node) =>
            (node.`type` == nodeType) &&
              !node.attributes.get(field.name).contains(fieldValue)
        }
        .values
        .toSet
    )

  override def filterByNodeTypes(
      nodeTypes: Set[NodeType]): Future[Set[NodeState]] = {
    Future.successful(
      map
        .filter { case (_, node) => nodeTypes.contains(node.`type`) }
        .values
        .toSet
    )

  }
}
