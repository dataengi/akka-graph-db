package com.dataengi.graphDb.dsl

import java.util.UUID

import com.dataengi.graphDb.actors.Node.{NodeId, NodeState, NodeType, RelationType}

import scala.concurrent.{ExecutionContext, Future}

trait GraphDSL {
  import Graph._

  trait GraphInterface {
    def +=(`type`: NodeType): Future[NodeId]

    def apply(id: NodeId): NodeRef

    def find(predicate: Predicate): Future[GraphNodes]

    def findIds(ids: Set[NodeId]): Future[GraphNodes]

    def getAllNodes: Future[GraphNodes]

    def getNodesByType(nodeType: NodeType): Future[GraphNodes]

    def nodeById(nodeId: NodeId): Future[NodeState]

    def removeRelation(id: NodeId, toId: NodeId): Future[Unit]

    def removeAttr(id: NodeId, name: String): Future[Unit]

    def relationIds(nodeId:NodeId, relationName:String): Future[Set[NodeId]]
  }

  case class GraphNodeParam(`type`: NodeType, kvs: Set[(String, Value)])

  implicit class GraphNodeBuildable(`type`: NodeType) {
    def within[T: ValueAlike](kv: (String, T)*): GraphNodeParam = {
      val im = implicitly[ValueAlike[T]]
      GraphNodeParam(`type`, kv.map(v => v._1 -> im.toValue(v._2)).toSet)
    }
  }

  class GraphDSLImpl(implicit gc: GraphContext, optimizer:QueryOptimizer, ec: ExecutionContext)
      extends GraphInterface {

    def +=(`type`: NodeType): Future[NodeId] = {
      val id = newNodeId()
      for { _ <- gc.createNode(id, `type`) } yield id
    }

    def +=(param: GraphNodeParam): Future[NodeId] = {
      val id = newNodeId()

      for {
        _ <- gc.createNode(id, param.`type`)
        _ <- gc.addAttrs(id, param.kvs)
      } yield id
    }

    private def newNodeId(): NodeId = UUID.randomUUID().toString

    override def apply(id: NodeId): NodeRef = NodeRef(id)

    override def find(predicate: Predicate): Future[GraphNodes] = {
      for { nodes <- optimizer.filterByPredicate(predicate)} yield GraphNodes(nodes)
    }

    override def findIds(ids: Set[NodeId]): Future[GraphNodes] = {
      for { nodes <- gc.nodeByIds(ids) } yield GraphNodes(nodes)
    }

    override def getNodesByType(nodeType: NodeType): Future[GraphNodes] = {
      for { nodes <- gc.filterByNodeType(nodeType) } yield GraphNodes(nodes)
    }

    override def getAllNodes: Future[GraphNodes] = {
      for { nodes <- gc.getAllNodes } yield GraphNodes(nodes)
    }

    override def nodeById(nodeId: NodeId): Future[NodeState] = {
      gc.nodeById(nodeId)
    }

    def removeRelation(id: NodeId, toId: NodeId): Future[Unit] = {
      gc.removeRelation(id, toId)
    }

    def removeAttr(id: NodeId, name: String): Future[Unit] = {
      gc.removeAttr(id, name)
    }

    override def relationIds(nodeId:NodeId, relationName:String): Future[Set[NodeId]] = {
      for { node <- gc.nodeById(nodeId) } yield node.relations.filter(value => value._2.contains(relationName)).keySet
    }

    trait GraphNodeInterface {

      def attr(name: String): Future[Value]

      def hasAttr(name: String): Future[Boolean]

      def nodeType: Future[NodeType]

      def hasRelation(toId: NodeId): Future[Boolean]

      def +=[T: ValueAlike](params: (String, T)): Future[Unit]

      def -=(attrName: String): Future[Unit]

      def ~>(to: NodeRef): Future[Unit]

      def ~>(relationType: RelationType): RelationBuilder

      def get(field: Field): Future[Value]

      def relationIds: Future[Set[NodeId]]
    }

    case class RelationParam(relationType: RelationType,
                             toId: NodeId)

    case class RelationBuilder(relationType: RelationType,
                               build: RelationParam => Future[Unit]) {
      def ~>(to: NodeRef): Future[Unit] =
        build(RelationParam(relationType, to.nodeId))
    }

    implicit class GraphNodeRefWrapper(node: NodeRef)
        extends GraphNodeInterface {

      override def +=[T: ValueAlike](params: (String, T)): Future[Unit] = {

        val value: Value = implicitly[ValueAlike[T]].toValue(params._2)

        gc.addAttr(node.nodeId, params._1, value)
      }

      override def -=(attrName: String): Future[Unit] =
        gc.removeAttr(node.nodeId, attrName)

      override def attr(name: String): Future[Value] =
        for { node <- gc.nodeById(node.nodeId) } yield node.attr(name)

      override def hasAttr(name: String): Future[Boolean] =
        for { node <- gc.nodeById(node.nodeId) } yield node.hasAttr(name)

      override def nodeType: Future[NodeType] =
        for { node <- gc.nodeById(node.nodeId) } yield node.nodeType

      override def ~>(to: NodeRef): Future[Unit] =
        gc.addRelation(node.nodeId, to.nodeId)

      override def ~>(relationType: RelationType): RelationBuilder =
        RelationBuilder(
          relationType,
          params =>
            gc.addRelation(node.nodeId, params.relationType, params.toId)
        )

      override def hasRelation(toId: NodeId): Future[Boolean] =
        for { node <- gc.nodeById(node.nodeId) } yield
          node.relations.contains(toId)

      override def get(field: Field): Future[Value] = {
        for { node <- gc.nodeById(node.nodeId) } yield node.get(field)
      }

      override def relationIds: Future[Set[NodeId]] = {
        for { node <- gc.nodeById(node.nodeId) } yield node.relations.keys.toSet
      }
    }

    implicit class GraphNodeWrapper(node: NodeState) {

      def nodeType: String = node.`type`

      def relation(relationType: RelationType): Set[NodeId] =
        node.relations.filter(_._2 == relationType).keys.toSet

      def get(field: Field): Value = {
        if (node.attributes.contains(field.name))
          node.attributes(field.name)
        else
          EmptyValue()
      }

      def attr(name: String): Value =
        if (node.attributes.contains(name))
          node.attributes(name)
        else
          EmptyValue()

      def hasAttr(name: String): Boolean =
        node.attributes.contains(name)

      def +=[T: ValueAlike](params: (String, T)): Future[Unit] = {

        val value: Value = implicitly[ValueAlike[T]].toValue(params._2)

        gc.addAttr(node.nodeId, params._1, value)
      }
    }
  }
}
