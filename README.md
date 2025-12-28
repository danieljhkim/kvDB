# KvDB - Distributed Key-Value Database

[![Build and Test](https://github.com/danieljhkim/Distributed-Key-Value-Database/actions/workflows/build.yml/badge.svg)](https://github.com/danieljhkim/Distributed-Key-Value-Database/actions/workflows/build.yml)

A Redis-like distributed key-value store implemented in Java with clustering capabilities.

## Features

This project provides a lightweight distributed in-memory database, interfaced with a CLI.
Supports a distributed architecture with a coordinator node and multiple storage nodes using modulo-based sharding strategy, enabling horizontal scalability.
The system ensures data durability through periodic disc persistence and offers basic fault tolerance with node failure recovery via WAL.

### Failure recovery

Failure of any node is managed by the coordinator node, by delegating failed node's work to another healthy node and, once the failed node is back online, it syncs the state via 2 WAL's:
- **Primary WAL**: Logs from the failed storage node
- **Secondary WAL**: Logs from the coordinator node that were kept while the node was down

### Client-Server Communication

- The server uses a combination of TCP sockets and gRPC for server-server communication
- Individual nodes can be accessed directly or through the coordinator


--- 

## Architecture Overview

KvDB follows a distributed architecture with the following components:

- **Coordinator Node**: Manages the cluster topology, routes client requests to appropriate nodes. Performs health checks and delegates tasks in case of node failures.
- **Storage Nodes**: Store the actual key-value data and handle read/write operations
- **Client Interface**: Connects to the coordinator for executing commands

```
         +-----------------------------+
         |        Client / CLI         |
         +-------------+---------------+
                       |
                       v
         +-------------+---------------+
         |       Coordinator Node      |
         |  - Knows cluster topology   |
         |  - Handles client requests  |
         |  - Routes to correct node   |
         +-------------+---------------+
                       |
       ----------------+------------------
      |                |                  |
+-----+-----+    +-----+-----+     +------+------+
|   Node A  |    |   Node B  |     |   Node C     |
| - KV store|    | - KV store|     | - KV store   |
+-----------+    +-----------+     +-------------+
```

--- 

## CLI Preview

![kvclient](assets/kvclient.png)

---

## Usage

### Prerequisites

- Java 11 or higher
- Maven

### Starting the Cluster

1. Package the project using Maven:

```bash
make build
````

2. Start the Cluster: Coordinator Server and Node Servers

```bash
make run-cluster 
```

3. Start the Client CLI

**Option 1** (recommended): [Use kvcli CLI Go application](./golang/kvcli/README.md)
```bash
make build-cli

# connect to the cluster
make run-cli
```

### Basic CLI Commands

#### In-Memory Store Operations

- `KV SET key value` - Set key to hold string value
- `KV GET key` - Get the value of key
- `KV DEL key` - Delete one or more keys


#### Other Commands

- `PING` - Test connection
- `HELP` - Show help message
- `EXIT` - Exit the client

--- 

## Configuration

Storage node configuration is done via `application.properties` file located in the `kv.common/src/main/resources/<node_id>` directory for locally running.

### File-based Persistence

The system supports file-based persistence with options for auto-flushing and custom file types.

```properties
kvdb.persistence.filepath=data/kvstore.dat
kvdb.persistence.filetype=dat
kvdb.persistence.enableAutoFlush=true
kvdb.persistence.autoFlushInterval=2
```

### Cluster Configuration

The coordinator uses a YAML configuration file to define the cluster topology, located in the `kv.coordinator/src/main/resources/cluster-config.yaml` file:

```yaml
nodes:
  - id: node1
    host: 127.0.0.1
    port: 8081
    useGrpc: true

  - id: node2
    host: 127.0.0.1
    port: 8082
    useGrpc: true

```

--- 

# License

This project is licensed under the MIT License.
