# Multi-stage build for Gateway. Maven 3.9.16 / Temurin 21 (pinned 2026-08-22).
FROM maven:3.9-eclipse-temurin-26-noble@sha256:b2c1ad85954592f9928e84327c65201f308ad9b5d8ed7d5b823717c97bf23fbb AS builder

WORKDIR /build

# Copy the entire project
COPY . .

# Build the project and package the gateway module
RUN mvn clean package -DskipTests -pl kv.gateway -am

# Runtime stage. Temurin 21.0.12_8 JRE (pinned 2026-08-22).
FROM eclipse-temurin:21.0.12.1_1-jre-resolute@sha256:e9a838377dc3719c0a0e7d1ac2e13d6f49c5cacd643f9a22024845736bb00ad9

WORKDIR /app

# Copy the built JAR from builder stage
COPY --from=builder /build/kv.gateway/target/kv-gateway.jar /app/Gateway.jar

EXPOSE 7000

# Expand KVDB_ENV at container startup, then replace the shell with the JVM so it receives SIGTERM.
CMD ["sh", "-c", "exec java -Dkvdb.env=${KVDB_ENV:-docker} -jar Gateway.jar"]
