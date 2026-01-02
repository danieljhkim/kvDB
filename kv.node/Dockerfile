# Multi-stage build for Node
FROM maven:3-eclipse-temurin-25 AS builder

WORKDIR /build

# Copy the entire project
COPY . .

# Build the project and package the node module
RUN mvn clean package -DskipTests -pl kv.node -am

# Runtime stage
FROM eclipse-temurin:21-jre

WORKDIR /app

# Create data directory
RUN mkdir -p /app/data

# Copy the built JAR from builder stage
COPY --from=builder /build/kv.node/target/kv-node.jar /app/Node.jar

EXPOSE 8001

# Use shell form to allow environment variable expansion
CMD java -Dkvdb.env=${KVDB_ENV:-docker} -jar Node.jar
