package com.dataengi.graphDb.dsl

import com.dataengi.graphDb.actors.Node.NodeType
import com.dataengi.graphDb.dsl.Graph.{Field, StringValue, Value}
import org.scalamock.scalatest.MockFactory
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContextExecutor

class QueryOptimizerSpec extends AnyWordSpecLike with MockFactory {

  "QueryOptimizer" should {

    "predicate NodeType" in new Fixture {
      (gc.filterByNodeType _).expects("Person").once()

      optimizer.filterByPredicate(HasNodeType("Person"))
    }

    "predicate Or NodeTypes" in new Fixture {
      (gc.filterByNodeTypes _).expects(Set("Person", "Business")).twice()

      optimizer.filterByPredicate(Or(HasNodeType("Person"), HasNodeType("Business")))
      optimizer.filterByPredicate(Or(HasNodeType("Business"), HasNodeType("Person")))
    }

    "predicate NodeType & Field" in new Fixture {
      (gc.filter(_: NodeType, _: Field, _: Value)).expects("Person", Field("city"), StringValue("Melbourne")).twice()

      optimizer.filterByPredicate(And(HasNodeType("Person"), Eq(Field("city"), StringValue("Melbourne"))))
      optimizer.filterByPredicate(And(Eq(Field("city"), StringValue("Melbourne")), HasNodeType("Person")))
    }

    "predicate NodeType & Not Field" in new Fixture {
      (gc
        .filterNotField(_: NodeType, _: Field, _: Value))
        .expects("Person", Field("city"), StringValue("Melbourne"))
        .repeated(4)

      optimizer.filterByPredicate(And(HasNodeType("Person"), Not(Eq(Field("city"), StringValue("Melbourne")))))
      optimizer.filterByPredicate(And(HasNodeType("Person"), Not(Eq(StringValue("Melbourne"), Field("city")))))
      optimizer.filterByPredicate(And(Not(Eq(Field("city"), StringValue("Melbourne"))), HasNodeType("Person")))
      optimizer.filterByPredicate(And(Not(Eq(StringValue("Melbourne"), Field("city"))), HasNodeType("Person")))
    }

    "predicate NodeType & Relation & Field" in new Fixture {

      (gc
        .filter(_: NodeType, _: HasRelation, _: Field, _: Value))
        .expects(
          "Person",
          HasRelation("EmployedBy", businessId1),
          Field("position"),
          StringValue("Architect")
        )
        .repeated(3)

      optimizer.filterByPredicate(
        And(And(HasNodeType("Person"), HasRelation("EmployedBy", businessId1)),
            Eq(Field("position"), StringValue("Architect"))))

      optimizer.filterByPredicate(
        And(And(HasRelation("EmployedBy", businessId1), HasNodeType("Person")),
            Eq(Field("position"), StringValue("Architect"))))

      optimizer.filterByPredicate(
        And(HasNodeType("Person"),
            And(HasRelation("EmployedBy", businessId1), Eq(Field("position"), StringValue("Architect")))))

    }

  }

  trait Fixture {

    implicit val gc: GraphContext             = mock[GraphContext]
    implicit val ec: ExecutionContextExecutor = concurrent.ExecutionContext.global

    val optimizer = new GraphContextQueryOptimizer()

    val businessId1 = "businessId1"
    val personId1   = "personId1"

  }

}
