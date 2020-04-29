package com.dataengi.graphDb.server

import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.{Done, actor => classic}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Server(routes: Route, port: Int, system: ActorSystem[_]) {
  import akka.actor.typed.scaladsl.adapter._
  implicit val classicSystem: classic.ActorSystem = system.toClassic
  private val shutdown = CoordinatedShutdown(classicSystem)

  import system.executionContext

  def start(): Unit = {
    Http().bindAndHandle(routes, "localhost", port).onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        system.log.info(
          "online at http://{}:{}/",
          address.getHostString,
          address.getPort
        )

        shutdown.addTask(
          CoordinatedShutdown.PhaseServiceRequestsDone,
          "http-graceful-terminate"
        ) { () =>
          binding.terminate(10.seconds).map { _ =>
            system.log
              .info(
                "http://{}:{}/ graceful shutdown completed",
                address.getHostString,
                address.getPort
              )
            Done
          }
        }
      case Failure(ex) =>
        system.log.error("Failed to bind HTTP endpoint, terminating system", ex)
        system.terminate()
    }
  }

}
