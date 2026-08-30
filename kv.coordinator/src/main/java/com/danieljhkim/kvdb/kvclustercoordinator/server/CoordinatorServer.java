package com.danieljhkim.kvdb.kvclustercoordinator.server;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftNode;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftSnapshotStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.rpc.RaftGrpcClient;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.rpc.RaftGrpcService;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachine;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachineImpl;
import com.danieljhkim.kvdb.kvclustercoordinator.service.CoordinatorServiceImpl;
import com.danieljhkim.kvdb.kvclustercoordinator.service.WatcherManager;
import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;
import com.danieljhkim.kvdb.kvcommon.grpc.InternalAuthServerInterceptor;
import com.danieljhkim.kvdb.kvcommon.grpc.InternalAuthToken;
import com.danieljhkim.kvdb.kvcommon.observability.AdmissionControlInterceptor;
import com.danieljhkim.kvdb.kvcommon.observability.CorrelationIdInterceptor;
import com.danieljhkim.kvdb.kvcommon.observability.HealthHttpServer;
import com.danieljhkim.kvdb.kvcommon.observability.Metrics;
import com.danieljhkim.kvdb.kvcommon.observability.RequestMetricsInterceptor;
import com.danieljhkim.kvdb.kvcommon.observability.ServiceLifecycle;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorServer {
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorServer.class);

    private final Server server;
    private final HealthHttpServer healthServer;
    private final ServiceLifecycle lifecycle = new ServiceLifecycle();
    private final Duration drainBudget;

    @Getter
    private final RaftStateMachine raftStateMachine;

    private final WatcherManager watcherManager;
    private final RaftNode raftNode;
    private final RaftGrpcClient raftGrpcClient;

    public CoordinatorServer(String nodeId, AppConfig appConfig) throws IOException {
        // Find this node's configuration
        AppConfig.NodeConfig thisNode = findNodeConfig(nodeId, appConfig);
        if (thisNode == null) {
            throw new IllegalArgumentException("Node configuration not found for nodeId: " + nodeId);
        }
        String internalToken = InternalAuthToken.require(appConfig);

        // Create Raft configuration
        RaftConfiguration raftConfig = createRaftConfiguration(nodeId, appConfig);

        // Initialize Raft persistence components
        Path dataDirPath = Paths.get(thisNode.getDataDir());
        Files.createDirectories(dataDirPath);

        RaftLog raftLog = new FileBasedRaftLog(dataDirPath.resolve("log"));
        RaftPersistentStateStore persistentStore =
                new RaftPersistentStateStore(dataDirPath.resolve("state").toString());
        RaftSnapshotStore snapshotStore = new RaftSnapshotStore(dataDirPath.resolve("snapshot"));

        // Initialize watcher manager
        this.watcherManager = new WatcherManager();

        // Initialize Raft state machine with watcher
        this.raftStateMachine = new RaftStateMachineImpl();
        this.raftStateMachine.addWatcher(watcherManager);

        // Initialize gRPC client for peer communication
        Map<String, String> peers = raftConfig.getPeers();
        this.raftGrpcClient = new RaftGrpcClient(nodeId, peers, internalToken);

        // Initialize RaftNode
        this.raftNode = new RaftNode(
                nodeId,
                raftConfig,
                raftLog,
                persistentStore,
                raftStateMachine,
                raftGrpcClient::sendRequestVote,
                raftGrpcClient::sendAppendEntries,
                raftGrpcClient::sendInstallSnapshot,
                snapshotStore);

        // When stepping down from leader, close all watcher connections
        // so clients reconnect to the new leader
        this.raftNode.setOnStepDownFromLeader(watcherManager::closeAllWatchers);

        // Create Raft gRPC service
        RaftGrpcService raftGrpcService = new RaftGrpcService(
                nodeId,
                raftNode.getVoteHandler(),
                raftNode::handleAppendEntries,
                raftNode::handleInstallSnapshot,
                raftNode::getCurrentTerm,
                raftNode::getCurrentLeader);

        // Create coordinator service
        CoordinatorServiceImpl coordinatorService =
                new CoordinatorServiceImpl(raftNode, raftStateMachine, watcherManager);
        InternalAuthServerInterceptor authInterceptor = new InternalAuthServerInterceptor(internalToken);
        GlobalExceptionInterceptor exceptionInterceptor = new GlobalExceptionInterceptor();
        ServerServiceDefinition interceptedCoordService = ServerInterceptors.intercept(
                coordinatorService,
                new CorrelationIdInterceptor(),
                new AdmissionControlInterceptor(lifecycle),
                new RequestMetricsInterceptor("coordinator", lifecycle),
                authInterceptor,
                exceptionInterceptor);
        ServerServiceDefinition interceptedRaftService = ServerInterceptors.intercept(
                raftGrpcService,
                new CorrelationIdInterceptor(),
                new RequestMetricsInterceptor("coordinator_raft", lifecycle),
                authInterceptor,
                exceptionInterceptor);

        // Build gRPC server with both services
        this.server = NettyServerBuilder.forPort(thisNode.getPort())
                .addService(interceptedCoordService)
                .addService(interceptedRaftService)
                .build();
        this.drainBudget = Duration.ofMillis(drainBudgetMillis());
        this.healthServer = new HealthHttpServer(
                healthPort(thisNode.getPort()),
                lifecycle,
                () -> raftNode.getCurrentLeader() != null
                        && !raftNode.getCurrentLeader().isBlank());
        Metrics.gauge("kvdb_raft_term", "coordinator", () -> raftNode.getCurrentTerm());
        Metrics.gauge("kvdb_raft_is_leader", "coordinator", () -> raftNode.isLeader() ? 1 : 0);
        Metrics.gauge("kvdb_raft_commit_index", "coordinator", () -> raftNode.getState()
                .getCommitIndex());
        Metrics.gauge(
                "kvdb_raft_apply_lag",
                "coordinator",
                () -> raftNode.getState().getCommitIndex() - raftNode.getState().getLastApplied());

        logger.info("Initialized CoordinatorServer with RaftNode: nodeId={}, port={}", nodeId, thisNode.getPort());
    }

    private AppConfig.NodeConfig findNodeConfig(String nodeId, AppConfig appConfig) {
        if (appConfig.getCoordinatorNodes() == null
                || appConfig.getCoordinatorNodes().getNodes() == null) {
            return null;
        }

        return appConfig.getCoordinatorNodes().getNodes().stream()
                .filter(node -> nodeId.equals(node.getId()))
                .findFirst()
                .orElse(null);
    }

    private RaftConfiguration createRaftConfiguration(String nodeId, AppConfig appConfig) {
        // Build cluster members map from coordinator nodes
        Map<String, String> clusterMembers = new HashMap<>();

        if (appConfig.getCoordinatorNodes() != null
                && appConfig.getCoordinatorNodes().getNodes() != null) {
            for (AppConfig.NodeConfig node : appConfig.getCoordinatorNodes().getNodes()) {
                String address = node.getHost() + ":" + node.getPort();
                clusterMembers.put(node.getId(), address);
            }
        }

        if (clusterMembers.isEmpty()) {
            logger.warn("No cluster members configured, running in single-node mode");
            // Use environment variable or default
            String host = System.getenv().getOrDefault("COORDINATOR_HOST", "localhost");
            String port = System.getenv().getOrDefault("COORDINATOR_PORT", "9000");
            clusterMembers.put(nodeId, host + ":" + port);
        }

        AppConfig.RaftConfig raftConfig = appConfig.getRaft();
        if (raftConfig == null) {
            raftConfig = new AppConfig.RaftConfig();
        }
        AppConfig.NodeConfig thisNode = findNodeConfig(nodeId, appConfig);
        String dataDir = thisNode != null ? thisNode.getDataDir() : "data/raft/" + nodeId;

        return RaftConfiguration.builder()
                .nodeId(nodeId)
                .clusterMembers(clusterMembers)
                .heartbeatInterval(Duration.ofMillis(raftConfig.getHeartbeatIntervalMs()))
                .electionTimeoutMin(Duration.ofMillis(raftConfig.getElectionTimeoutMinMs()))
                .electionTimeoutMax(Duration.ofMillis(raftConfig.getElectionTimeoutMaxMs()))
                .maxEntriesPerAppendRequest(raftConfig.getMaxEntriesPerRequest())
                .snapshotThreshold(raftConfig.getSnapshotThreshold())
                .dataDirectory(dataDir)
                .build();
    }

    public void start() throws IOException, InterruptedException {
        logger.info("Starting CoordinatorServer...");
        raftNode.start();

        logger.info("RaftNode started");
        watcherManager.start();

        server.start();
        healthServer.start();
        logger.info("gRPC server started on port {}", server.getPort());

        server.awaitTermination();
    }

    public CompletableFuture<Void> submitCommand(RaftCommand command) {
        return raftNode.submitCommand(command);
    }

    public void shutdown() throws InterruptedException {
        logger.info("Shutting down CoordinatorServer...");
        lifecycle.beginDrain();
        boolean drained = false;

        if (server != null) {
            server.shutdown();
            drained = lifecycle.awaitDrain(drainBudget);
            if (!server.awaitTermination(drainBudget.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)) {
                logger.warn("Coordinator RPC drain timed out after {} ms", drainBudget.toMillis());
                server.shutdownNow();
            }
            logger.info("Coordinator RPC drain outcome: drained={}", drained);
        }

        if (raftNode != null) {
            raftNode.stop();
            logger.info("RaftNode stopped");
        }

        if (raftGrpcClient != null) {
            raftGrpcClient.close();
            logger.info("RaftGrpcClient closed");
        }

        if (watcherManager != null) {
            watcherManager.stop();
            logger.info("WatcherManager stopped");
        }
        healthServer.close();

        logger.info("CoordinatorServer shutdown complete");
    }

    private static int healthPort(int grpcPort) {
        return Integer.parseInt(System.getenv().getOrDefault("KVDB_HEALTH_PORT", Integer.toString(grpcPort + 10_000)));
    }

    private static long drainBudgetMillis() {
        return Long.parseLong(System.getenv().getOrDefault("KVDB_DRAIN_TIMEOUT_MS", "30000"));
    }
}
