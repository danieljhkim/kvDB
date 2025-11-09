# -----------------------------------
# Variables
# -----------------------------------

MVN := mvn
GOCLI := golang/kvcli
COORD := kv.coordinator
NODE := kv.server

# -----------------------------------
# Targets
# -----------------------------------

all: clean java run_cluster run_cli

# -----------------
# Build Java: uses Maven only
# -----------------
java:
	@echo "Running Maven build for all modules..."
	$(MVN) clean package
	cp $(COORD)/target/kv.coordinator-*.jar $(COORD)/Coordinator.jar
	cp $(NODE)/target/kv.server-*.jar $(NODE)/Node.jar

# -----------------
# Build Go CLI
# -----------------
build_cli:
	@echo "Building Go CLI..."
	cd $(GOCLI) && go build -o kv

# -----------------
# run Go CLI
# -----------------
run_cli:
	@echo "running kv CLI..."
	kv connect --host localhost --port 7000

# -----------------
# Clean everything
# -----------------
clean:
	@echo "Cleaning Maven build artifacts..."
	$(MVN) clean
	@echo "Cleaning Go binary..."
	rm -f $(GOCLI)/kv
	rm -f $(COORD)/Coordinator.jar
	rm -f $(NODE)/Node.jar
	rm -f $(COORD)/kv-coordinator.pid
	rm -f $(COORD)/logs/*.log
	./scripts/kill_ports.sh 8081 8082 7000

# -----------------
# cluster commands
# -----------------
run_cluster:
	chmod +x scripts/run_client.sh scripts/run_cluster.sh scripts/cluster-server.sh
	./scripts/cluster-server.sh start

stop_cluster:
	./scripts/kill_ports.sh 8081 8082 7000

logs_cluster:
	./scripts/cluster-server.sh logs

status_cluster:
	./scripts/cluster-server.sh status

.PHONY: all java cli clean run_cluster stop_cluster