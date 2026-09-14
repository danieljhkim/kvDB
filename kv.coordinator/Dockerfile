# Multi-stage build for Coordinator. Maven 3.9.16 / Temurin 21 (pinned 2026-08-22).
FROM maven:3.9.16-eclipse-temurin-21-noble@sha256:613124833fa6718ded9d655a2ebfab6425818c178f899116b93560b6f1c9ffe9 AS builder

WORKDIR /build

# Copy the entire project
COPY . .

# Build the project and package the coordinator module
RUN mvn clean package -DskipTests -pl kv.coordinator -am

# Runtime stage. Temurin 21.0.12_8 JRE (pinned 2026-08-22).
FROM eclipse-temurin:21.0.12_8-jre-resolute@sha256:d422afbc2c66d57d5aaf8fc4f51898d1b77941ff42fa12f222950269f42ae3c1

WORKDIR /app

# Copy the built JAR from builder stage
COPY --from=builder /build/kv.coordinator/target/kv-coordinator.jar /app/Coordinator.jar

EXPOSE 9000

# Use shell form to allow environment variable expansion
CMD java -Dkvdb.env=${KVDB_ENV:-docker} -jar Coordinator.jar
