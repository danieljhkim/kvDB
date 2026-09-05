# -----------------------------------
# Variables
# -----------------------------------

MVN := mvn
GOCLI := golang/kvcli
COORD := kv.coordinator
NODE := kv.node
GATEWAY := kv.gateway

# Go CLI protocol generation. Pinned so regeneration is reproducible.
PROTOC ?= protoc
PROTO_DIR := kv.proto/src/main/proto
PROTOC_GEN_GO_VERSION := v1.36.6
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1
PROTO_TOOLS_DIR ?= $(HOME)/.cache/kvdb/proto-tools
GO_GATEWAY_PACKAGE := github.com/danieljhkim/kv/internal/gen/kvdb/gateway
GO_GATEWAY_OUT := $(GOCLI)/internal/gen/kvdb/gateway

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
# Go CLI
# -----------------
proto-tools:
	@echo "Installing pinned protoc plugins into $(PROTO_TOOLS_DIR)..."
	GOBIN=$(PROTO_TOOLS_DIR) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(PROTO_TOOLS_DIR) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

# Regenerates the Go gateway bindings from the authoritative proto. Rerunning
# this with the pinned plugin versions produces byte-identical output.
proto-go: proto-tools
	@echo "Generating Go bindings for $(PROTO_DIR)/kvgateway.proto..."
	mkdir -p $(GO_GATEWAY_OUT)
	PATH="$(PROTO_TOOLS_DIR):$$PATH" $(PROTOC) -I $(PROTO_DIR) \
		--go_out=$(GO_GATEWAY_OUT) \
		--go_opt=paths=source_relative \
		--go_opt=Mkvgateway.proto=$(GO_GATEWAY_PACKAGE) \
		--go-grpc_out=$(GO_GATEWAY_OUT) \
		--go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mkvgateway.proto=$(GO_GATEWAY_PACKAGE) \
		kvgateway.proto

go-build:
	cd $(GOCLI) && go build -o kv

go-test:
	cd $(GOCLI) && go test -race ./...

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

.PHONY: all build clean proto-tools proto-go go-build go-test run-cluster run-gateway stop bootstrap-cluster smoke-test logs cluster-status wipe-data format lint k6-gateway-bench k6-admin-bench ghz-gateway-bench vegeta-admin-bench