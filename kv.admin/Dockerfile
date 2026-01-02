# Multi-stage build for Admin
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /build

# Copy the entire project
COPY . .

# Build the project and package the admin module
RUN mvn clean package -DskipTests -pl kv.admin -am

# Runtime stage
FROM eclipse-temurin:21-jre

WORKDIR /app

# Copy the built JAR from builder stage
COPY --from=builder /build/kv.admin/target/kv-admin.jar /app/Admin.jar

EXPOSE 8089

# Use shell form to allow environment variable expansion
CMD java -Dkvdb.env=${KVDB_ENV:-docker} -jar Admin.jar
