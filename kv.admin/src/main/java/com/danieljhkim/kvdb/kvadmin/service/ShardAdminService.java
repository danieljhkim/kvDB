package com.danieljhkim.kvdb.kvadmin.service;

import com.danieljhkim.kvdb.kvadmin.api.dto.KeyPlacementDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ResolveKeyRequestDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.TriggerRequestDto;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorAdminClient;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import com.danieljhkim.kvdb.kvadmin.config.AdminServerConfig;
import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.exception.PayloadTooLargeException;
import com.danieljhkim.kvdb.kvcommon.limits.KvRequestLimits;
import com.google.protobuf.ByteString;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Service for shard administration operations.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ShardAdminService {

    private final CoordinatorAdminClient coordinatorAdminClient;
    private final CoordinatorReadClient coordinatorReadClient;
    private final AdminServerConfig adminServerConfig;

    public List<ShardDto> listShards() {
        com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto shardMap = coordinatorReadClient.getShardMap();
        if (shardMap == null || shardMap.getShards() == null) {
            throw new IllegalStateException("Shard map not available: cannot list shards");
        }
        return shardMap.getShards().values().stream().collect(Collectors.toList());
    }

    public ShardDto getShard(String shardId) {
        com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto shardMap = coordinatorReadClient.getShardMap();
        if (shardMap == null || shardMap.getShards() == null) {
            throw new IllegalStateException("Shard map not available: cannot get shard " + shardId);
        }
        ShardDto shard = shardMap.getShards().get(shardId);
        if (shard == null) {
            throw new IllegalArgumentException("Shard not found: " + shardId);
        }
        return shard;
    }

    public ShardDto setShardReplicas(String shardId, List<String> replicaNodeIds) {
        coordinatorAdminClient.setShardReplicas(shardId, replicaNodeIds);
        return getShard(shardId);
    }

    public ShardDto setShardLeader(String shardId, String leaderNodeId) {
        ShardDto currentShard = getShard(shardId);
        coordinatorAdminClient.setShardLeader(shardId, currentShard.getEpoch(), leaderNodeId);
        return getShard(shardId);
    }

    public TriggerRequestDto triggerRebalance(TriggerRequestDto request) {
        // TODO: Implement rebalancing logic
        log.info("Triggering rebalance: {}", request);
        return request;
    }

    /**
     * Decode a base64 key and return coordinator placement at observation time. Does not hash the
     * key or read/write its value.
     */
    public KeyPlacementDto resolveKeyPlacement(ResolveKeyRequestDto request) {
        if (request == null || request.getKeyBase64() == null) {
            throw new IllegalArgumentException("key_base64 is required");
        }
        String encoded = request.getKeyBase64();
        int maxKeyBytes = adminServerConfig.getMaxKeyBytes();
        int maxEncodedChars = maxEncodedKeyChars(maxKeyBytes);
        if (encoded.length() > maxEncodedChars) {
            throw new PayloadTooLargeException("encoded key exceeds configured limit (actual=" + encoded.length()
                    + ", max=" + maxEncodedChars + ")");
        }
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Malformed base64-encoded key");
        }
        AppConfig.LimitsConfig limitsConfig = new AppConfig.LimitsConfig();
        limitsConfig.setMaxKeyBytes(maxKeyBytes);
        new KvRequestLimits(limitsConfig).validateKey(ByteString.copyFrom(decoded));

        log.info("Resolving key placement; decoded_key_bytes={}", decoded.length);
        ShardDto shard = coordinatorReadClient.resolveShard(decoded);
        log.info(
                "Resolved key placement shard_id={} epoch={} leader={}",
                shard.getShardId(),
                shard.getEpoch(),
                shard.getLeader());
        return KeyPlacementDto.builder()
                .shardId(shard.getShardId())
                .epoch(shard.getEpoch())
                .replicas(shard.getReplicas())
                .leader(shard.getLeader())
                .configState(shard.getConfigState())
                .build();
    }

    static int maxEncodedKeyChars(int maxKeyBytes) {
        return ((maxKeyBytes + 2) / 3) * 4;
    }
}
