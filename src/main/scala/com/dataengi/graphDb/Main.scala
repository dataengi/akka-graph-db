package com.dataengi.graphDb

import java.util.concurrent.CountDownLatch

import akka.actor.typed.ActorSystem
import akka.cluster.typed.Cluster
import com.dataengi.graphDb.actors.Guardian
import com.dataengi.graphDb.server.InitDb
import com.typesafe.config.{Config, ConfigFactory}

object Main {

  def main(args: Array[String]): Unit = {
    args.headOption match {

      case Some(portString) if portString.matches("""\d+""") =>
        val port = portString.toInt
        val httpPort = ("80" + portString.takeRight(2)).toInt
        startNode(port, httpPort)

      case Some("cassandra") =>
        InitDb.startCassandraDatabase()
        println("Started Cassandra, press Ctrl + C to kill")
        new CountDownLatch(1).await()

      case None =>
        throw new IllegalArgumentException(
          "port number, or cassandra required argument"
        )
    }
  }

  private def startNode(port: Int, httpPort: Int): Unit = {
    val system =
      ActorSystem[Nothing](Guardian(), "Node", config(port, httpPort))

    if (Cluster(system).selfMember.hasRole("read-model"))
      InitDb.createReadModelTables(system)
  }

  private def config(port: Int, httpPort: Int): Config =
    ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port = $port
      http.port = $httpPort
       """).withFallback(ConfigFactory.load())
}