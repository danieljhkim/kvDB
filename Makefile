# -----------------------------------
# Variables
# -----------------------------------

MVN := mvn
GOCLI := golang/kvcli
COORD := kv.coordinator
NODE := kv.node
GATEWAY := kv.gateway

# -----------------------------------
# Targets
# -----------------------------------

all: clean build run_cluster

# -----------------
# Build Java: uses Maven only
# -----------------
build:
	@echo "Running Maven build for all modules..."
	$(MVN) clean package -DskipTests

# -----------------
# Clean everything
# -----------------
clean:
	@echo "Cleaning Maven build artifacts..."
	$(MVN) clean
	rm -rf logs/*
	./scripts/run_cluster.sh stop

# -----------------
# cluster commands
# -----------------
run-cluster:
	chmod +x scripts/run_cluster.sh
	./scripts/run_cluster.sh

run-gateway:
	@echo "Starting Gateway..."
	java -jar $(GATEWAY)/target/kv-gateway.jar

stop:
	./scripts/run_cluster.sh stop

bootstrap-cluster:
	@echo "Bootstrapping coordinator (registering nodes and initializing shards)..."
	chmod +x scripts/bootstrap_cluster.sh
	./scripts/bootstrap_cluster.sh

smoke-test:
	@echo "Running integration smoke test (bootstrap + Put/Get)..."
	chmod +x scripts/smoke_test.sh
	./scripts/smoke_test.sh

logs:
	@echo "Tailing logs... Ctrl + C to exit."
	tail -f logs/*

cluster-status:
	./scripts/run_cluster.sh status

wipe-data:
	rm -rf data/*
	@echo "Data directory wiped."


# -----------------
# docker commands
# -----------------

docker-build:
    docker build -t kvcoordinator:latest -f docker/Coordinator.Dockerfile .
    docker build -t kvnode:latest -f docker/Node.Dockerfile .
    docker build -t kvgateway:latest -f docker/Gateway.Dockerfile .

# -----------------
# Format and lint
# -----------------
format:
	mvn spotless:apply

lint:
	mvn spotless:check

# -----------------
# Benchmark commands
# -----------------
k6-gateway-bench:
	chmod +x benchmark/scripts/run_k6_gateway.sh
	./benchmark/scripts/run_k6_gateway.sh

k6-admin-bench:
	chmod +x benchmark/scripts/run_k6_admin.sh
	./benchmark/scripts/run_k6_admin.sh

ghz-gateway-bench:
	chmod +x benchmark/scripts/run_ghz_gateway.sh
	./benchmark/scripts/run_ghz_gateway.sh

vegeta-admin-bench:
	chmod +x benchmark/scripts/run_vegeta_admin.sh
	./benchmark/scripts/run_vegeta_admin.sh

.PHONY: all build clean run-cluster run-gateway stop bootstrap-cluster smoke-test logs cluster-status wipe-data format lint k6-gateway-bench k6-admin-bench ghz-gateway-bench vegeta-admin-bench