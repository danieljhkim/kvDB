# Multi-stage build for Admin. Maven 3.9.16 / Temurin 21 (pinned 2026-08-22).
FROM maven:3-eclipse-temurin-26-noble@sha256:166ca19b6b5fe1e924ab2d66b64ba9854c739f16210b94bbe0074b036c5c7992 AS builder

WORKDIR /build

# Copy the entire project
COPY . .

# Build the project and package the admin module
RUN mvn clean package -DskipTests -pl kv.admin -am

# Runtime stage. Temurin 21.0.12_8 JRE (pinned 2026-08-22).
FROM eclipse-temurin:21.0.12_8-jre-resolute@sha256:097b5c0e8b5c9cc402e871a87a35f20e9413af9159410db2b1bdd8b78dcca7ed

WORKDIR /app

# Copy the built JAR from builder stage
COPY --from=builder /build/kv.admin/target/kv-admin.jar /app/Admin.jar

EXPOSE 8089

# Use shell form to allow environment variable expansion
CMD java -Dkvdb.env=${KVDB_ENV:-docker} -jar Admin.jar
