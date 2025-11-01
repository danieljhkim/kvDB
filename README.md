# KvDB - Distributed Key-Value Database

A Redis-like distributed key-value store implemented in Java with clustering capabilities.

## Features

This project provides a lightweight, in-memory database with periodic disk persistence via simple CLI commands.
Supports a distributed architecture with a coordinator node and multiple data nodes using write-ahead logs (WAL) and a modulo-based sharding strategy, enabling horizontal scalability.

### Failure recovery

Failure of any node is managed by the coordinator node, by delegating failed node's work to another healthy node and, once the failed node is back online, it syncs the state via 2 WAL's:
- **Primary WAL**: Logs from the failed node
- **Secondary WAL**: Logs from the coordinator node that were kept while the node was down

### Client-Server Communication

- The server uses a combination of TCP sockets and gRPC for server-server communication
- Individual nodes can be accessed directly or through the coordinator


--- 

## Architecture Overview

KvDB follows a distributed architecture with the following components:

- **Coordinator Node**: Manages the cluster topology, routes client requests to appropriate nodes
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
| - Replica |    | - Replica |     | - Replica    |
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
make java
````

2. Start the Cluster: Coordinator Server and Node Servers

```bash
make run_cluster 
```

3. Start the Client CLI

**Option 1** (recommended): [Use kvcli CLI Go application](./golang/kvcli/README.md)
```bash
make cli

# connect to the cluster
kv connect --host localhost --port 7000
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

Storage node configuration is done via `application.properties` file located in the `kv.common/src/main/resources/<node_id>` directory.

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
