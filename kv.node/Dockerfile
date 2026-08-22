# Multi-stage build for Node. Maven 3.9.16 / Temurin 21 (pinned 2026-08-22).
FROM maven:3-eclipse-temurin-24-noble@sha256:a137a467ec89b5713d0be817b55bdba6b4d6ef16e3d05565a79bc08d8e775a1c AS builder

WORKDIR /build

# Copy the entire project
COPY . .

# Build the project and package the node module
RUN mvn clean package -DskipTests -pl kv.node -am

# Runtime stage. Temurin 21.0.12_8 JRE (pinned 2026-08-22).
FROM eclipse-temurin:21.0.12_8-jre-resolute@sha256:097b5c0e8b5c9cc402e871a87a35f20e9413af9159410db2b1bdd8b78dcca7ed

WORKDIR /app

# Create data directory
RUN mkdir -p /app/data

# Copy the built JAR from builder stage
COPY --from=builder /build/kv.node/target/kv-node.jar /app/Node.jar

EXPOSE 8001

# Use shell form to allow environment variable expansion
CMD java -Dkvdb.env=${KVDB_ENV:-docker} -jar Node.jar
