package com.dataengi.graphDb.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.typed.Cluster
import com.dataengi.graphDb.dsl.GraphContext
import com.dataengi.graphDb.dsl.impl.AkkaClusterGraph
import com.dataengi.graphDb.readmodel._
import com.dataengi.graphDb.server.{Routes, Server}

object Guardian {
  def apply(): Behavior[Nothing] = {
    Behaviors.setup[Nothing] { context =>
      implicit val system: ActorSystem[Nothing] = context.system
      implicit val graphContext: GraphContext = new AkkaClusterGraph
      val httpPort = context.system.settings.config.getInt("http.port")
      val settings = EventProcessorSettings(system)

      Node.init(system, settings)

      if (Cluster(system).selfMember.hasRole("read-model")) {
        // FIXME, the tables may not be created yet, send a start message they're done
        EventProcessor.init(
          system,
          settings,
          tag =>
            new NodeEventProcessorStream(
              system,
              system.executionContext,
              settings.id,
              tag
          )
        )
      }

      val routes = new Routes()
      new Server(routes.route, httpPort, context.system).start()

      Behaviors.empty
    }
  }
}
