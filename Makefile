# -----------------------------------
# Variables
# -----------------------------------

MVN := mvn
GOCLI := golang/kvcli
COORD := kv.coordinator
NODE := kv.node

# -----------------------------------
# Targets
# -----------------------------------

all: clean build run_cluster run_cli

# -----------------
# Build Java: uses Maven only
# -----------------
build:
	@echo "Running Maven build for all modules..."
	$(MVN) clean package

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


.PHONY: all build clean run_cluster stop_cluster logs cluster_status wipe_data build_cli run_cli