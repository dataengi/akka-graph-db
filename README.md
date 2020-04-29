
# Graph DB Concept Implemented on Akka

This is a concept of implementation of the Graph Database Engine on Akka. It provides simple Domain Specific Language (DSL) for Graph operations.   
Graph nodes are represented as Actors. The architecture of the applications is based on CQRS and Event Sourcing design patterns.     

Summary:
- CQRS/ES
- Decentralized architecture
- Apache Cassandra as storage
- Akka persistence
- Sharding support
- Async graph DSL
- Pluggable implementation of DSL on Akka or in-memory
- The basic graph query optimizer


## Implementation Notes

This implementation based on CQRS Command Query Responsibility Segregation and Event Sourcing patterns.
Commands (writes) are separated from queries (reads). Apache Cassandra is used as storage for the Event log and read model. This storage is natively supported by Akka (alpakka).

We should point out that this CQRS/ES design could have eventual consistency in the case of reading after writing.

### Points for Improvement

Because Apache Cassandra is used for read model there are ways to improve queries - Cassandra is not so flexible for querying.
On top level for Graph operations implemented QueryOptimizer that translates Graph query to optimized Cassandra version.
In the case when optimizer can't find optimization is used an expensive call (querying all actors to execute predicate).
There are two implementations of Graph operations for plugging to DSL.
One is based on Akka and another is the in-memory implementation for testing purposes. We started from synchronous DSL and switched to asynchronous for Akka support.
DSL style is simple enough to express Graph operations concisely.
Complex Graph update queries are implemented as a series of calls. It is possible to make it more atomic or even transactional.

## Run tests

To run all tests:

```
sbt test
```

There are the following tests:

- NodeSpec - tests of interaction with Graph logic via Actors
- GraphDSLSpec - tests of asynchronous DSL (decoupled from Graph implementation)
- OperationsDSLSpec - tests of implementation of high-level logic in DSL
- QueryOptimizerSpec - tests of the basic Graph query optimizer


## Running the REST Service Cluster

The application is cluster-ready. Here are commands to start embedded Cassandra service and two application nodes.

1. Start a Cassandra server by running:

```
sbt "runMain com.dataengi.graphDb.Main cassandra"
```

2. Start a node that runs the write model:

```
sbt -Dakka.cluster.roles.0=write-model "runMain com.dataengi.graphDb.Main 2553"
```

3. Start a node that runs the read model:

```
sbt -Dakka.cluster.roles.0=read-model "runMain com.dataengi.graphDb.Main 2554"
```

## Interact With the Application via REST

Add node in the Graph:

```
curl -X POST -H "Content-Type: application/json" -d '{"nodeType":"person", "fields":[{"valueType":"string", "name":"lastname", "value":"Jack"}, {"valueType":"string", "name":"firstname", "value":"Nik"}, {"valueType":"number", "name":"age", "value":"20"}]}' http://127.0.0.1:8053/nodes
```

Example of result:

`7dac801b-a657-4df2-9325-3986763aed1f`


Get the node of the Graph:

```
curl http://127.0.0.1:8053/nodes/{NODE_ID}
```

Example of result:

```json
{"attributes":{"age":20.0,"firstname":"Nik","lastname":"Jack"},"nodeId":"7dac801b-a657-4df2-9325-3986763aed1f","relations":{},"type":"person"}
```

Add field to the Graph:

```
curl -X PUT -H "Content-Type: application/json" -d '{"valueType":"string", "name":"name", "value":"Jack"}' http://127.0.0.1:8053/nodes/fields/{NODE_ID}
```

Example of result:

`OK`

Remove the field of the node:

```
curl -X DELETE -H "Content-Type: application/json" -d '{"name":"name"}' http://127.0.0.1:8053/nodes/fields/{NODE_ID}
```

Example of result:

`OK`
