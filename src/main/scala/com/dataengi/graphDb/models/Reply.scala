package com.dataengi.graphDb.models

import com.dataengi.graphDb.actors.Node

trait Reply extends CborSerializable

final case class ReplyError(message: String) extends Reply

final case class SuccessReply(state: Node.NodeState) extends Reply
