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
- PostgreSQL (optional, for SQL persistence)

### Starting the Cluster

1. Package the project using Maven:

```bash
mvn clean package
````

2. Make the scripts executable:

```bash
chmod +x scripts/run_client.sh scripts/run_cluster.sh
```

3. Start the Cluster: Coordinator Server and Node Servers

```bash
# bash script to start the coordinator and node servers
./scripts/run_cluster.sh
```

4. Start the Client CLI

**Option 1**: Use kv.client Java application

```bash
./scripts/run_client.sh
```

**Option 2** (recommended): [Use kvcli CLI Go application](./golang/kvcli/README.md)
```bash
cd kvcli

go build -o kv

# move the binary PATH

# for unix:
mv kv /usr/local/bin/

# for windows:
move kv.exe C:\Windows\System32\

# connect to the cluster
kv connect --host localhost --port 7000
```

### Basic CLI Commands

#### In-Memory Store Operations

- `KV SET key value` - Set key to hold string value
- `KV GET key` - Get the value of key
- `KV DEL key` - Delete one or more keys

#### TODO'S:

- `KV EXISTS key` - Check if key(s) exist
- `KV ALL` - Get all key-value pairs (not yet)
- `KV DROP` - Remove all keys from store and disk
- `KV SIZE` - Get the number of keys in the store
- `SAVE` - Save the current state to disk


#### Other Commands

- `PING` - Test connection
- `HELP` - Show help message
- `EXIT` - Exit the client

--- 

## Configuration

Configuration is done via `application.properties` file located in the `src/main/resources` directory.

### Cluster Configuration

The coordinator uses a YAML configuration file to define the cluster topology:

```yaml
nodes:
  - id: node1
    host: localhost
    port: 7001
    isGrpc: true
    grpcPort: 9001
  - id: node2
    host: localhost
    port: 7002
    isGrpc: true
    grpcPort: 9002
```

### File-based Persistence

The system supports file-based persistence with options for auto-flushing and custom file types.

```properties
kvdb.persistence.filepath=data/kvstore.dat
kvdb.persistence.filetype=dat
kvdb.persistence.enableAutoFlush=true
kvdb.persistence.autoFlushInterval=2
```

### PostgreSQL Persistence

SQL commands leverage PostgreSQL for persistent storage.

```properties
kvdb.database.default.url=jdbc:postgresql://localhost:5432/kvdb
kvdb.database.default.driver=org.postgresql.Driver
kvdb.database.default.username=yourusername
kvdb.database.default.password=yourpassword
kvdb.database.default.table=kv_store
```

### Server Configuration

The server can be configured to run on a specific host and port.

```properties
kvdb.server.port=6379
kvdb.server.host=localhost
```

--- 

# License

This project is licensed under the MIT License.
