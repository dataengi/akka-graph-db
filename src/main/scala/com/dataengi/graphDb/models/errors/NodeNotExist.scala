package com.dataengi.graphDb.models.errors

import com.dataengi.graphDb.actors.Node.NodeId

case class NodeNotExist(nodeId: NodeId) extends Exception {
  override def getMessage: NodeId = s"Node $nodeId does not exist"
}
