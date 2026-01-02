# Multi-stage build for Gateway
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /build

# Copy the entire project
COPY . .

# Build the project and package the gateway module
RUN mvn clean package -DskipTests -pl kv.gateway -am

# Runtime stage
FROM eclipse-temurin:21-jre

WORKDIR /app

# Copy the built JAR from builder stage
COPY --from=builder /build/kv.gateway/target/kv-gateway.jar /app/Gateway.jar

EXPOSE 7000

# Use shell form to allow environment variable expansion
CMD java -Dkvdb.env=${KVDB_ENV:-docker} -jar Gateway.jar
