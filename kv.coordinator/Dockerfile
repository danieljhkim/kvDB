# Multi-stage build for Coordinator
FROM maven:3-eclipse-temurin-25 AS builder

WORKDIR /build

# Copy the entire project
COPY . .

# Build the project and package the coordinator module
RUN mvn clean package -DskipTests -pl kv.coordinator -am

# Runtime stage
FROM eclipse-temurin:21-jre

WORKDIR /app

# Copy the built JAR from builder stage
COPY --from=builder /build/kv.coordinator/target/kv-coordinator.jar /app/Coordinator.jar

EXPOSE 9000

# Use shell form to allow environment variable expansion
CMD java -Dkvdb.env=${KVDB_ENV:-docker} -jar Coordinator.jar
