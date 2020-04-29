package com.dataengi.graphDb.dsl

import com.dataengi.graphDb.actors.Node.{NodeId, NodeState, NodeType, RelationType}

import scala.concurrent.Future

trait GraphContext {
  import Graph._

  def nodeById(id: NodeId): Future[NodeState]

  def nodeByIds(ids: Set[NodeId]): Future[Set[NodeState]]

  def getAllNodes: Future[Set[NodeState]]

  def filter(func: NodeState => Boolean): Future[Set[NodeState]]

  def filterByNodeType(nodeType: NodeType): Future[Set[NodeState]]

  def filterByNodeTypes(nodeTypes: Set[NodeType]): Future[Set[NodeState]]

  def filter(nodeType: NodeType, relation: HasRelation, field: Field, fieldValue: Value): Future[Set[NodeState]]

  def filter(nodeType: NodeType, relation: HasRelation): Future[Set[NodeState]]

  def filter(nodeType: NodeType, relationType: RelationType): Future[Set[NodeState]]

  def filter(nodeType: NodeType, field: Field, fieldValue: Value): Future[Set[NodeState]]

  def filterNotField(nodeType: NodeType, field: Field, fieldValue: Value): Future[Set[NodeState]]

  def filter(nodeId: NodeId): Future[Set[NodeState]]

  def createNode(id: NodeId, `type`: NodeType): Future[Unit]

  def addAttr(id: NodeId, name: String, value: Value): Future[Unit]

  def addAttrs(id: NodeId, attrs: Set[(String, Value)]): Future[Unit]

  def removeAttr(id: NodeId, name: String): Future[Unit]

  def addRelation(id: NodeId, toId: NodeId): Future[Unit]

  def addRelation(id: NodeId, relationType: RelationType, toId: NodeId): Future[Unit]

  def removeRelation(id: NodeId, toId: NodeId): Future[Unit]
}
