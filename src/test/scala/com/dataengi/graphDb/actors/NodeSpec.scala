package com.dataengi.graphDb.actors

import java.util.UUID

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.dataengi.graphDb.dsl.Graph.{NumberValue, StringValue}
import com.dataengi.graphDb.models
import com.dataengi.graphDb.models._
import org.scalatest.wordspec.AnyWordSpecLike

class NodeSpec
    extends ScalaTestWithActorTestKit(s"""
      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
      akka.persistence.snapshot-store.local.dir = "target/snapshot-${UUID
      .randomUUID()
      .toString}"
    """)
    with AnyWordSpecLike {

  "Entity" should {

    "create node" in new Fixture {

      val person = testKit.spawn(Node(nodeId, Set()))

      val probe = testKit.createTestProbe[Reply]

      val fields = Map("age" -> NumberValue(20))

      person ! Create(nodeId, "Person", fields, probe.ref)

      probe.expectMessage(SuccessReply(Node.NodeState(nodeId, "Person", fields)))
    }

    "create node error: node is already created" in new Fixture {

      val person = testKit.spawn(Node(nodeId, Set()))

      val probe = testKit.createTestProbe[Reply]

      val fields = Map("age" -> NumberValue(20))

      person ! models.Create(nodeId, "Person", fields, ignoreRef)

      person ! models.Create(nodeId, "Person", fields, probe.ref)

      probe.expectMessage(ReplyError("Node is already created!"))
    }

    "add fields to node" in new Fixture {

      val person = testKit.spawn(Node(nodeId, Set()))

      val probe = testKit.createTestProbe[Reply]

      val fields = Map("age" -> NumberValue(20))

      val newFields = Map("name" -> StringValue("John"))

      person ! models.Create(nodeId, "Person", fields, ignoreRef)

      person ! AddFields(newFields, probe.ref)

      probe.expectMessage(
        SuccessReply(
          Node.NodeState(nodeId, "Person", Map("age" -> NumberValue(20), "name" -> StringValue("John")))
        )
      )

    }

    "remove field from node" in new Fixture {

      val person = testKit.spawn(Node(nodeId, Set()))

      val probe = testKit.createTestProbe[Reply]

      val fields = Map("age" -> NumberValue(20), "name" -> StringValue("John"))

      person ! models.Create(nodeId, "Person", fields, ignoreRef)

      person ! RemoveField("name", probe.ref)

      probe.expectMessage(
        SuccessReply(Node.NodeState(nodeId, "Person", Map("age" -> NumberValue(20))))
      )

    }
  }

  trait Fixture {
    val ignoreRef = system.ignoreRef[Reply]

    def uniquePersonId(): String = UUID.randomUUID().toString

    val age1 = 25
    val age2 = 30

    val nodeId  = uniquePersonId()
    val nodeId2 = uniquePersonId()

    val friendId1 = uniquePersonId()
    val friendId2 = uniquePersonId()
    val friendId3 = uniquePersonId()

  }

}
