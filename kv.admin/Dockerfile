# Multi-stage build for Admin. Maven 3.9.16 / Temurin 21 (pinned 2026-08-22).
FROM maven:3.9-eclipse-temurin-26-noble@sha256:b2c1ad85954592f9928e84327c65201f308ad9b5d8ed7d5b823717c97bf23fbb AS builder

WORKDIR /build

# Copy the entire project
COPY . .

# Build the project and package the admin module
RUN mvn clean package -DskipTests -pl kv.admin -am

# Runtime stage. Temurin 21.0.12_8 JRE (pinned 2026-08-22).
FROM eclipse-temurin:21.0.12.1_1-jre-resolute@sha256:e7b42d0fad9b3dd6d61d6816ce3984df079a5da7d8e9af81c464243d15c04e3e

WORKDIR /app

# Copy the built JAR from builder stage
COPY --from=builder /build/kv.admin/target/kv-admin.jar /app/Admin.jar

EXPOSE 8089

# Use shell form to allow environment variable expansion
CMD java -Dkvdb.env=${KVDB_ENV:-docker} -jar Admin.jar
