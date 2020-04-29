package com.dataengi.graphDb.dsl

import java.util.UUID

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.dataengi.graphDb.actors.Node.NodeId
import com.dataengi.graphDb.dsl.Graph.{Field, NumberValue, StringValue}
import com.dataengi.graphDb.dsl.impl.InMemoryGraphContext
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.language.postfixOps

class OperationsDSLSpec
    extends ScalaTestWithActorTestKit(s"""
      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
      akka.persistence.snapshot-store.local.dir = "target/snapshot-${UUID
      .randomUUID()
      .toString}"
    """)
    with AnyWordSpecLike {

  import com.dataengi.graphDb.dsl.ValueInstances._

  "Operations" should {

    "All friends of a given person" in new Fixture {

      import graph._

      for {
        personId1 <- graph += "Person"
        friendId1 <- graph += "Person"
        friendId2 <- graph += "Person"
        friendId3 <- graph += "Person"
        _         <- graph(personId1) ~> graph(friendId1)
        _         <- graph(personId1) ~> graph(friendId2)
        _         <- graph(personId1) ~> graph(friendId3)
      } yield {
        whenReady(graph(personId1) relationIds)(_ shouldEqual Set(friendId1, friendId2, friendId3))
      }

    }

    "List all employed persons" in new Fixture {

      import graph._

      for {
        personId1   <- graph += "Person"
        _           <- graph += "Person"
        personId3   <- graph += "Person"
        _           <- graph += "Person"
        businessId1 <- graph += "Business" within ("name" -> "Facebook")
        _           <- graph(personId1) ~> "EmployedBy" ~> graph(businessId1)
        _           <- graph(personId3) ~> "EmployedBy" ~> graph(businessId1)
      } yield {
        whenReady(graph find And(HasNodeType("Person"), HasRelationType("EmployedBy"))) {
          _.nodes.map(_.nodeId) shouldEqual Set(personId1, personId3)
        }
      }

    }

    "List all persons employed by a business located in the area X" in new Fixture {

      import graph._

      for {
        personId1   <- graph += "Person" within ("name" -> "Alice")
        _           <- graph += "Person"
        personId3   <- graph += "Person" within ("name" -> "Bob")
        _           <- graph += "Person"
        businessId1 <- graph += "Business" within ("name" -> "Google", "location" -> "US")
        _           <- graph(businessId1) ~> "Employee" ~> graph(personId1)
        _           <- graph(businessId1) ~> "Employee" ~> graph(personId3)

      } yield {
        whenReady(graph find And(Eq(Field("location"), StringValue("US")), HasNodeType("Business"))) { nodes =>
          val employeeIds: Set[NodeId] =
            nodes.nodes.flatMap(_.relation("Employee"))

          employeeIds shouldEqual Set(personId1, personId3)

          whenReady(graph findIds employeeIds)(
            _.nodes.map(_.get(Field("name"))) shouldEqual Set(StringValue("Alice"), StringValue("Bob"))
          )
        }
      }
    }

    "[Optional] Grant a 10% salary increase to everyone employed by company Y and holding a position Z" in new Fixture {

      import graph._

      for {
        personId1   <- graph += "Person" within ("name" -> "Alice", "position" -> "Architect")
        _           <- graph(personId1) += "salary" -> 100000
        personId2   <- graph += "Person" within ("name" -> "Bob", "position" -> "Developer")
        _           <- graph(personId2) += "salary" -> 50000
        businessId1 <- graph += "Business" within ("name" -> "Apple")
        _           <- graph(businessId1) ~> "Employee" ~> graph(personId1)
        _           <- graph(businessId1) ~> "Employee" ~> graph(personId2)
        _           <- graph(personId1) ~> "EmployedBy" ~> graph(businessId1)
        _           <- graph(personId2) ~> "EmployedBy" ~> graph(businessId1)
        employees <- graph find And(And(HasNodeType("Person"), HasRelation("EmployedBy", businessId1)),
                                    Eq(Field("position"), StringValue("Architect")))

      } yield {
        employees.nodeIds shouldEqual Set(personId1)

        val salaryMult = 1.1

        for { employee <- employees.nodes } yield {

          for {
            _ <- employee attr "salary" match {
                  case NumberValue(salary) =>
                    employee += "salary" -> (BigDecimal(salary) * salaryMult).toDouble
                  case _ => Future.unit
                }
          } yield {
            whenReady(graph(personId1) get Field("salary"))(_ shouldEqual NumberValue(110000))
            whenReady(graph(personId2) get Field("salary"))(_ shouldEqual NumberValue(50000))
          }
        }

      }
    }
  }

  trait Fixture {

    implicit val ex: ExecutionContextExecutor = system.executionContext
    implicit val graphContext: GraphContext   = InMemoryGraphContext()
    implicit val qo: QueryOptimizer           = new GraphContextQueryOptimizer()
    val graph                                 = new GraphDSLImpl()

  }

}
