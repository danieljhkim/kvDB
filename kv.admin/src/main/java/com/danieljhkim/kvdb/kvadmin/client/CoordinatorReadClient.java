package com.danieljhkim.kvdb.kvadmin.client;

import com.danieljhkim.kvdb.kvadmin.api.dto.*;
import com.danieljhkim.kvdb.kvcommon.exception.ShardMapUnavailableException;
import com.danieljhkim.kvdb.kvcommon.grpc.InternalAuthChannels;
import com.danieljhkim.kvdb.proto.coordinator.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC client for Coordinator read operations with leader discovery.
 */
public class CoordinatorReadClient {

    private static final Logger logger = LoggerFactory.getLogger(CoordinatorReadClient.class);
    private static final int MAX_RETRIES = 5;
    private static final Pattern HOST = Pattern.compile(
            "[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*");

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, CoordinatorGrpc.CoordinatorBlockingStub> stubs = new ConcurrentHashMap<>();
    private final List<String> coordinatorAddresses;
    private final AtomicReference<String> leaderAddress = new AtomicReference<>();
    private final long timeoutSeconds;
    private final ChannelFactory channelFactory;

    public CoordinatorReadClient(String host, int port, long timeout, TimeUnit timeUnit) {
        this(List.of(host + ":" + port), timeout, timeUnit);
    }

    public CoordinatorReadClient(List<String> coordinatorAddresses, long timeout, TimeUnit timeUnit) {
        this(coordinatorAddresses, timeout, timeUnit, InternalAuthChannels::forAddress);
    }

    CoordinatorReadClient(
            List<String> coordinatorAddresses, long timeout, TimeUnit timeUnit, ChannelFactory channelFactory) {
        this.coordinatorAddresses = new ArrayList<>(coordinatorAddresses);
        this.timeoutSeconds = timeUnit.toSeconds(timeout);
        this.channelFactory = channelFactory;
        logger.info("CoordinatorReadClient created for: {}", coordinatorAddresses);
    }

    @FunctionalInterface
    interface ChannelFactory {
        ManagedChannel create(String host, int port);
    }

    private CoordinatorGrpc.CoordinatorBlockingStub getStub(String address) {
        return stubs.computeIfAbsent(address, addr -> {
                    ManagedChannel channel = channels.computeIfAbsent(addr, a -> {
                        String[] parts = splitUsableAddress(a);
                        return channelFactory.create(parts[0], Integer.parseInt(parts[1]));
                    });
                    return CoordinatorGrpc.newBlockingStub(channel);
                })
                .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
    }

    @SuppressWarnings("all")
    private CoordinatorGrpc.CoordinatorBlockingStub getLeaderStub() {
        // Verify cached leader is still the leader
        String leader = leaderAddress.get();
        if (leader != null) {
            try {
                GetCoordinatorLeaderResponse response =
                        getStub(leader).getCoordinatorLeader(GetCoordinatorLeaderRequest.getDefaultInstance());
                if (response.getIsLeader()) {
                    return getStub(leader);
                }
                logger.info("Cached leader {} is no longer leader", leader);
                leaderAddress.set(null);
            } catch (StatusRuntimeException e) {
                logger.debug("Failed to verify cached leader {}: {}", leader, e.getStatus());
                leaderAddress.set(null);
            }
        }

        // Discover leader by querying coordinators
        for (String address : coordinatorAddresses) {
            try {
                GetCoordinatorLeaderResponse response =
                        getStub(address).getCoordinatorLeader(GetCoordinatorLeaderRequest.getDefaultInstance());
                if (response.getIsLeader()) {
                    leaderAddress.set(address);
                    logger.info("Discovered leader: {}", address);
                    return getStub(address);
                }
                String hint = usableAddress(response.getLeaderAddress());
                if (hint != null) {
                    // Verify the hinted leader
                    try {
                        GetCoordinatorLeaderResponse hintResponse =
                                getStub(hint).getCoordinatorLeader(GetCoordinatorLeaderRequest.getDefaultInstance());
                        if (hintResponse.getIsLeader()) {
                            leaderAddress.set(hint);
                            logger.info("Discovered and verified leader via hint: {}", hint);
                            if (!coordinatorAddresses.contains(hint)) {
                                coordinatorAddresses.add(hint);
                            }
                            return getStub(hint);
                        }
                    } catch (StatusRuntimeException e) {
                        logger.debug("Failed to verify hinted leader {}: {}", hint, e.getStatus());
                    }
                }
            } catch (StatusRuntimeException e) {
                logger.debug("Failed to query {}: {}", address, e.getStatus());
            }
        }

        logger.warn("Could not discover leader, using first coordinator");
        return getStub(coordinatorAddresses.getFirst());
    }

    private <T> T execute(Function<CoordinatorGrpc.CoordinatorBlockingStub, T> operation) {
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                return operation.apply(getLeaderStub());
            } catch (StatusRuntimeException e) {
                Status.Code code = e.getStatus().getCode();
                if (code == Status.Code.FAILED_PRECONDITION || code == Status.Code.UNAVAILABLE) {
                    String hint = extractLeaderHintFromDescription(e.getStatus().getDescription());
                    if (hint != null) {
                        logger.info("Extracted leader hint: {}", hint);
                        leaderAddress.set(hint);
                        if (!coordinatorAddresses.contains(hint)) {
                            coordinatorAddresses.add(hint);
                        }
                    } else {
                        leaderAddress.set(null);
                    }
                    logger.info("Leader unavailable, retrying ({}/{})", attempt + 1, MAX_RETRIES);
                    try {
                        Thread.sleep(100 * (attempt + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new StatusRuntimeException(Status.CANCELLED.withDescription("Interrupted"));
                    }
                    continue;
                }
                throw e;
            }
        }
        throw new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("Failed after " + MAX_RETRIES + " attempts"));
    }

    private String extractLeaderHintFromDescription(String description) {
        if (description == null) return null;
        String prefix = "Leader hint: ";
        int idx = description.indexOf(prefix);
        return idx >= 0
                ? usableAddress(description.substring(idx + prefix.length()).trim())
                : null;
    }

    private String usableAddress(String address) {
        return splitUsableAddressOrNull(address) == null ? null : address;
    }

    private String[] splitUsableAddress(String address) {
        String[] parts = splitUsableAddressOrNull(address);
        if (parts == null) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Invalid coordinator address")
                    .asRuntimeException();
        }
        return parts;
    }

    private String[] splitUsableAddressOrNull(String address) {
        if (address == null || !address.equals(address.trim())) return null;

        int separator = address.lastIndexOf(':');
        if (separator <= 0 || separator != address.indexOf(':')) return null;

        String host = address.substring(0, separator);
        String portText = address.substring(separator + 1);
        if (!HOST.matcher(host).matches() || portText.isEmpty() || portText.length() > 5) return null;

        int port = 0;
        for (int i = 0; i < portText.length(); i++) {
            char digit = portText.charAt(i);
            if (digit < '0' || digit > '9') return null;
            port = port * 10 + (digit - '0');
        }
        return port >= 1 && port <= 65535 ? new String[] {host, portText} : null;
    }

    public ShardMapSnapshotDto getShardMap() {
        return execute(stub -> {
            GetShardMapResponse response = stub.getShardMap(
                    GetShardMapRequest.newBuilder().setIfVersionGt(0).build());

            if (!response.hasState()) {
                throw new IllegalStateException("Shard map not available");
            }
            return convertToDto(response.getState());
        });
    }

    public List<NodeDto> listNodes() {
        return execute(stub -> {
            ListNodesResponse response =
                    stub.listNodes(ListNodesRequest.newBuilder().build());
            return response.getNodesList().stream().map(this::convertNode).collect(Collectors.toList());
        });
    }

    public NodeDto getNode(String nodeId) {
        return execute(stub -> {
            GetNodeResponse response =
                    stub.getNode(GetNodeRequest.newBuilder().setNodeId(nodeId).build());
            if (!response.hasNode()) {
                throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("Node not found: " + nodeId));
            }
            return convertNode(response.getNode());
        });
    }

    /**
     * Resolve coordinator placement for a binary key. The key bytes are forwarded to ResolveShard
     * as-is; this client does not hash, stringify, or read the key's value.
     */
    public ShardDto resolveShard(byte[] key) {
        ByteString keyBytes = ByteString.copyFrom(key);
        return execute(stub -> {
            ResolveShardResponse response = stub.resolveShard(
                    ResolveShardRequest.newBuilder().setKey(keyBytes).build());
            if (!response.hasShard()) {
                throw new ShardMapUnavailableException("Coordinator did not resolve a shard for the requested key");
            }
            return convertShard(response.getShard());
        });
    }

    private ShardMapSnapshotDto convertToDto(ClusterState state) {
        Map<String, NodeDto> nodes = state.getNodesMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> convertNode(e.getValue())));

        Map<String, ShardDto> shards = state.getShardsMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> convertShard(e.getValue())));

        PartitioningConfigDto partitioning = null;
        if (state.hasPartitioning()) {
            partitioning = PartitioningConfigDto.builder()
                    .numShards(state.getPartitioning().getNumShards())
                    .replicationFactor(state.getPartitioning().getReplicationFactor())
                    .build();
        }

        return ShardMapSnapshotDto.builder()
                .mapVersion(state.getMapVersion())
                .nodes(nodes)
                .shards(shards)
                .partitioning(partitioning)
                .build();
    }

    private NodeDto convertNode(NodeRecord node) {
        return NodeDto.builder()
                .nodeId(node.getNodeId())
                .address(node.getAddress())
                .zone(node.getZone())
                .rack(node.getRack())
                .status(node.getStatus().name())
                .lastHeartbeatMs(node.getLastHeartbeatMs())
                .capacityHints(node.getCapacityHintsMap())
                .build();
    }

    private ShardDto convertShard(ShardRecord shard) {
        KeyRangeDto keyRange = null;
        if (shard.hasKeyRange()) {
            keyRange = KeyRangeDto.builder()
                    .startKey(shard.getKeyRange().getStartKey().toByteArray())
                    .endKey(shard.getKeyRange().getEndKey().toByteArray())
                    .build();
        }

        return ShardDto.builder()
                .shardId(shard.getShardId())
                .epoch(shard.getEpoch())
                .replicas(shard.getReplicasList())
                .leader(shard.getLeader())
                .configState(shard.getConfigState().name())
                .keyRange(keyRange)
                .build();
    }

    public void shutdown() {
        logger.info("Shutting down CoordinatorReadClient");
        channels.values().forEach(ch -> {
            try {
                ch.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                ch.shutdownNow();
                Thread.currentThread().interrupt();
            }
        });
        channels.clear();
        stubs.clear();
    }
}
