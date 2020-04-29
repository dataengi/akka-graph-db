package com.dataengi.graphDb.server

import java.io.File

import akka.Done
import akka.actor.typed.ActorSystem
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object InitDb {

  def startCassandraDatabase(): Unit = {
    val databaseDirectory = new File("target/cassandra-db")
    CassandraLauncher.start(
      databaseDirectory,
      CassandraLauncher.DefaultTestConfigResource,
      clean = false,
      port = 9042
    )
  }

  def createReadModelTables(system: ActorSystem[Nothing]): Future[Done] = {
    val session =
      CassandraSessionRegistry(system).sessionFor("alpakka.cassandra")

    val keyspaceStmt =
      """
      CREATE KEYSPACE IF NOT EXISTS read_model
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
      """

    val offsetTableStmt =
      """
      CREATE TABLE IF NOT EXISTS read_model.offsetStore (
        eventProcessorId text,
        tag text,
        timeUuidOffset timeuuid,
        PRIMARY KEY (eventProcessorId, tag)
      )
      """

    val nodeTableStmt =
      """
      CREATE TABLE read_model.nodes (
        type text,
        nodeId text,
        attributes map<text, text>,
        relations map<text, text>,
        PRIMARY KEY (nodeId)
      )
      """

    Await.ready(session.executeDDL(keyspaceStmt), 30.seconds)
    Await.ready(session.executeDDL(offsetTableStmt), 30.seconds)
    Await.ready(session.executeDDL(nodeTableStmt), 30.seconds)
  }
}
