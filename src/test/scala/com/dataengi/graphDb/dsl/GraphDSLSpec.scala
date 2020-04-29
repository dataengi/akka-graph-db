package com.dataengi.graphDb.dsl

import java.util.UUID

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.dataengi.graphDb.dsl.Graph._
import com.dataengi.graphDb.dsl.impl.InMemoryGraphContext
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContextExecutor
import scala.language.postfixOps

class GraphDSLSpec
    extends ScalaTestWithActorTestKit(s"""
      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
      akka.persistence.snapshot-store.local.dir = "target/snapshot-${UUID
      .randomUUID()
      .toString}"
    """)
    with AnyWordSpecLike {

  import com.dataengi.graphDb.dsl.ValueInstances._

  "General GraphDSL" should {

    "Create a data node of a given type" in new Fixture {

      import graph._

      for {
        nodeId <- graph += "Person"
      } yield {
        whenReady(graph(nodeId) nodeType)(_ shouldEqual "Person")
      }
    }

    "Add a new attribute to an existing node" in new Fixture {

      import graph._

      for {
        nodeId <- graph += "Person"
        _      <- graph(nodeId) += "name" -> "value"
      } yield {
        whenReady(graph(nodeId) attr "name")(_ shouldEqual StringValue("value"))
      }
    }

    "Create a data node with attributes" in new Fixture {

      import graph._

      for {
        nodeId <- graph += "Person" within ("name" -> "value", "city" -> "Kyiv")
      } yield {
        whenReady(graph(nodeId) attr "name")(_ shouldEqual StringValue("value"))
        whenReady(graph(nodeId) attr "city")(_ shouldEqual StringValue("Kyiv"))
      }
    }

    "Delete an attribute from an existing node" in new Fixture {

      import graph._

      for {
        nodeId <- graph += "Person"
        _      <- graph(nodeId) += "attr1" -> "value1"
        _      <- graph(nodeId) += "attr2" -> "value2"
        _      <- graph(nodeId) -= "attr1"
      } yield {
        whenReady(graph(nodeId) hasAttr "attr1")(_ shouldEqual false)
        whenReady(graph(nodeId) attr "attr2")(_ shouldEqual StringValue("value2"))
      }
    }

    "Establish a directional link between a two nodes in the system" in new Fixture {

      import graph._

      for {
        nodeId1 <- graph += "Person"
        nodeId2 <- graph += "Person"
        _       <- graph(nodeId1) ~> graph(nodeId2)
      } yield {
        whenReady(graph(nodeId1) hasRelation nodeId2)(_ shouldEqual true)
        whenReady(graph(nodeId2) hasRelation nodeId1)(_ shouldEqual false)
      }
    }
  }

  "API querying Predicate" should {
    "OR(X, Y) where X, Y are Predicate" in new Fixture {

      import graph._

      for {
        _ <- graph += "Person"
        _ <- graph += "Business"

      } yield {
        whenReady(graph find Or(HasNodeType("Person"), HasNodeType("Business")))(
          _.nodes.map(_.nodeType) shouldEqual Set("Person", "Business"))
      }
    }

    "AND(X, Y) where X, Y are Predicate" in new Fixture {

      import graph._

      for {
        personId1 <- graph += "Person"
        personId2 <- graph += "Person"
        _         <- graph(personId1) += "city" -> "Melbourne"
        _         <- graph(personId2) += "city" -> "NY"
      } yield {
        whenReady(graph find And(HasNodeType("Person"), Eq(Field("city"), StringValue("Melbourne"))))(
          _.nodes.map(_.attr("city")) shouldEqual Set(StringValue("Melbourne")))
      }

    }

    "NOT(X) where X is Predicate" in new Fixture {

      import graph._

      for {
        personId1 <- graph += "Person"
        personId2 <- graph += "Person"
        _         <- graph(personId1) += "city" -> "Melbourne"
        _         <- graph(personId2) += "city" -> "NY"
      } yield {
        whenReady(graph find And(HasNodeType("Person"), Not(Eq(Field("city"), StringValue("Melbourne")))))(
          _.nodes.map(_.attr("city")) shouldEqual Set(StringValue("Melbourne")))
      }
    }

    "EQ(X, Y) where X, Y are Value" in new Fixture {

      import graph._

      for {
        personId1 <- graph += "Person"
        personId2 <- graph += "Person"
        _         <- graph(personId1) += "city" -> "Melbourne"
        _         <- graph(personId2) += "city" -> "NY"
      } yield {
        whenReady(graph find And(HasNodeType("Person"), Eq(Field("city"), StringValue("Melbourne"))))(
          _.nodes.map(_.attr("city")) shouldEqual Set(StringValue("Melbourne")))
      }
    }

    "HAS_RELATION(X) where X is a Relation ID" in new Fixture {

      import graph._

      for {
        personId1 <- graph += "Person"
        personId2 <- graph += "Person"
        _         <- graph(personId1) += "city" -> "Melbourne"
        _         <- graph(personId2) += "city" -> "NY"
        _         <- graph(personId1) ~> graph(personId2)

      } yield {
        whenReady(graph(personId1) hasRelation personId2)(_ shouldEqual true)
        whenReady(graph(personId2) hasRelation personId1)(_ shouldEqual false)
      }
    }
  }

  "API querying Value" should {
    "FIELD(X) where X is the name of an attribute defined on the node" in new Fixture {

      import graph._

      for {
        personId1 <- graph += "Person"
        _         <- graph(personId1) += "interest" -> "Akka"
      } yield {
        whenReady(graph(personId1) get Field("interest"))(_ shouldEqual StringValue("Akka"))
      }
    }

    "BOOL(x) where X is scala.Boolean" in new Fixture {

      import graph._

      for {
        personId1 <- graph += "Person"
        _         <- graph(personId1) += "sporty" -> true
      } yield {
        whenReady(graph(personId1) get Field("sporty"))(_ shouldEqual BoolValue(true))
      }
    }

    "NUMBER(X) where X is scala.Double" in new Fixture {

      import graph._

      for {
        personId1 <- graph += "Person"
        _         <- graph(personId1) += "weight" -> 75.0
      } yield {
        whenReady(graph(personId1) get Field("weight"))(_ shouldEqual NumberValue(75.0))
      }
    }

    "STR(X) where X is scala.String" in new Fixture {

      import graph._

      for {
        personId1 <- graph += "Person"
        _         <- graph(personId1) += "hobby" -> "books"
      } yield {
        whenReady(graph(personId1) get Field("hobby"))(_ shouldEqual StringValue("books"))
      }
    }

    "LST(X) where X is Values" in new Fixture {

      import graph._

      for {
        personId1 <- graph += "Person"
        _         <- graph(personId1) += "clouds" -> Seq("aws", "gcp", "azure", "ibm", "alibaba")
        _         <- graph(personId1) += "numbers" -> Seq(10.0, 192.0)
      } yield {
        whenReady(graph(personId1) get Field("clouds"))(
          _ shouldEqual ListValue(
            Set(
              StringValue("aws"),
              StringValue("gcp"),
              StringValue("azure"),
              StringValue("ibm"),
              StringValue("alibaba")
            )))

        whenReady(graph(personId1) get Field("numbers"))(_ shouldEqual ListValue(Set(NumberValue(10.0), NumberValue(192.0))))
      }

    }

  }

  trait Fixture {

    implicit val ex: ExecutionContextExecutor = system.executionContext
    implicit val graphContext: GraphContext   = InMemoryGraphContext()
    implicit val qo: QueryOptimizer           = new GraphContextQueryOptimizer()

    val graph = new GraphDSLImpl()

  }

}
