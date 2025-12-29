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

all: clean build run_cluster run_cli

# -----------------
# Build Java: uses Maven only
# -----------------
build:
	@echo "Running Maven build for all modules..."
	$(MVN) clean package -DskipTests

# -----------------
# Build Go CLI
# -----------------
build-cli:
	@echo "Building Go CLI..."
	cd $(GOCLI) && go build -o kv

# -----------------
# run Go CLI
# -----------------
run-cli:
	@echo "running kv CLI..."
	kv connect --host localhost --port 7000

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

# -----------------
# Gateway commands
# -----------------
run-gateway:
	@echo "Starting Gateway..."
	java -jar $(GATEWAY)/target/kv-gateway.jar

logs:
	@echo "Tailing logs... Ctrl + C to exit."
	tail -f logs/*

cluster-status:
	./scripts/run_cluster.sh status

wipe-data:
	rm -rf data/*
	@echo "Data directory wiped."

format:
	mvn spotless:apply

lint:
	mvn spotless:check


.PHONY: all build clean run_cluster stop_cluster logs cluster_status wipe_data build_cli run_cli run_gateway bootstrap_cluster smoke_test