package com.dataengi.graphDb.dsl.impl

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.util.Timeout
import com.dataengi.graphDb.dsl.Graph._
import com.dataengi.graphDb.dsl._
import com.dataengi.graphDb.actors.Node
import com.dataengi.graphDb.actors.Node.{NodeId, NodeState, NodeType, RelationType}
import com.dataengi.graphDb.models._
import com.dataengi.graphDb.models.errors.NodeNotExist
import com.dataengi.graphDb.readmodel.ReadModelDAO

import scala.concurrent.{ExecutionContext, Future}

class AkkaClusterGraph(implicit private val system: ActorSystem[_])
    extends GraphContext {
  private val sharding = ClusterSharding(system)
  private val readModelDAO: ReadModelDAO = new ReadModelDAO

  implicit private val timeout: Timeout =
    Timeout.create(system.settings.config.getDuration("askTimeout"))
  implicit val ec: ExecutionContext = system.executionContext

  override def nodeByIds(ids: Set[NodeId]): Future[Set[NodeState]] =
    readModelDAO.getByIds(ids)

  override def getAllNodes: Future[Set[NodeState]] =
    readModelDAO.getAllNodes.runFold(Set.empty[NodeState])(_ + _)

  override def filter(func: NodeState => Boolean): Future[Set[NodeState]] =
    readModelDAO.getAllNodes
      .filter(func)
      .runFold(Set.empty[NodeState])(_ + _)

  override def filter(nodeType: NodeType,
                      relation: HasRelation,
                      field: Field,
                      fieldValue: Value): Future[Set[NodeState]] =
    readModelDAO
      .getByTypeAndFieldAndRelation(
        nodeType,
        field.name,
        fieldValue,
        relation.relationType,
        relation.relationId
      )
      .runFold(Set.empty[NodeState])(_ + _)

  override def filter(nodeType: NodeType,
                      relation: HasRelation): Future[Set[NodeState]] =
    readModelDAO
      .getByTypeAndRelation(
        nodeType,
        relation.relationType,
        relation.relationId
      )
      .runFold(Set.empty[NodeState])(_ + _)

  override def filter(nodeType: NodeType,
                      relationType: RelationType): Future[Set[NodeState]] =
    readModelDAO
      .getByTypeAndRelationType(nodeType, relationType)
      .runFold(Set.empty[NodeState])(_ + _)

  override def filter(nodeType: NodeType,
                      field: Field,
                      fieldValue: Value): Future[Set[NodeState]] =
    readModelDAO
      .getByTypeAndField(nodeType, field.name, fieldValue)
      .runFold(Set.empty[NodeState])(_ + _)

  override def filterNotField(nodeType: NodeType,
                              field: Field,
                              fieldValue: Value): Future[Set[NodeState]] =
    readModelDAO
      .getByTypeAndNotField(nodeType, field.name, fieldValue)
      .runFold(Set.empty[NodeState])(_ + _)

  override def filter(nodeId: NodeId): Future[Set[NodeState]] =
    nodeById(nodeId).map(Set(_))

  override def nodeById(id: NodeId): Future[NodeState] =
    readModelDAO.getNode(id).flatMap {
      case Some(node) => Future.successful(node)
      case None       => Future.failed(NodeNotExist(id))
    }

  override def createNode(id: NodeId, `type`: NodeType): Future[Unit] = {
    val entityRef = sharding.entityRefFor(Node.EntityKey, id)
    entityRef.ask[Reply](Create(id, `type`, Map.empty, _)).map(_ => ())
  }

  override def addAttr(id: NodeId, name: String, value: Value): Future[Unit] = {
    val entityRef = sharding.entityRefFor(Node.EntityKey, id)
    entityRef
      .ask[Reply](AddField(name, value, _))
      .map(_ => ())
  }

  override def addAttrs(id: NodeId,
                        attrs: Set[(String, Value)]): Future[Unit] = {
    val entityRef = sharding.entityRefFor(Node.EntityKey, id)
    val attrMap = attrs.map(v => v._1 -> v._2).toMap
    entityRef
      .ask[Reply](AddFields(attrMap, _))
      .map(_ => ())
  }

  override def removeAttr(id: NodeId, name: String): Future[Unit] = {
    val entityRef = sharding.entityRefFor(Node.EntityKey, id)
    entityRef
      .ask[Reply](RemoveField(name, _))
      .map(_ => ())
  }

  override def addRelation(id: NodeId, toId: NodeId): Future[Unit] = {
    val entityRef = sharding.entityRefFor(Node.EntityKey, id)
    entityRef
      .ask[Reply](UpdateRelation("default", toId, _))
      .map(_ => ())
  }

  override def addRelation(id: NodeId,
                           relationType: RelationType,
                           toId: NodeId): Future[Unit] = {
    val entityRef = sharding.entityRefFor(Node.EntityKey, id)
    entityRef
      .ask[Reply](UpdateRelation(relationType, toId, _))
      .map(_ => ())
  }

  override def removeRelation(id: NodeId, toId: NodeId): Future[Unit] = {
    val entityRef = sharding.entityRefFor(Node.EntityKey, id)
    entityRef
      .ask[Reply](RemoveRelation(id, toId, _))
      .map(_ => ())
  }

  override def filterByNodeType(nodeType: NodeType): Future[Set[NodeState]] = {
    readModelDAO
      .getByType(nodeType)
      .runFold(Set.empty[NodeState])(_ + _)
  }

  override def filterByNodeTypes(
    nodeTypes: Set[NodeType]
  ): Future[Set[NodeState]] = {
    readModelDAO
      .getByNodeTypes(nodeTypes)
      .runFold(Set.empty[NodeState])(_ + _)
  }

}
