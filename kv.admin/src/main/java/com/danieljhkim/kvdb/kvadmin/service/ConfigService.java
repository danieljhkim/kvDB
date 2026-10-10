package com.danieljhkim.kvdb.kvadmin.service;

import com.danieljhkim.kvdb.kvadmin.client.CoordinatorAdminClient;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Service for configuration operations.
 */
@Service
@RequiredArgsConstructor
public class ConfigService {

    private final CoordinatorAdminClient coordinatorAdminClient;
    private final CoordinatorReadClient coordinatorReadClient;

    public Map<String, Object> getConfig() {
        com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto shardMap = coordinatorReadClient.getShardMap();
        if (shardMap == null) {
            throw new IllegalStateException("Shard map not available: cannot get config");
        }
        return Map.of(
                "map_version",
                shardMap.getMapVersion(),
                "num_shards",
                shardMap.getPartitioning() != null ? shardMap.getPartitioning().getNumShards() : 0,
                "replication_factor",
                shardMap.getPartitioning() != null ? shardMap.getPartitioning().getReplicationFactor() : 0);
    }

    public Map<String, Object> updateConfig(Map<String, Object> config) {
        throw new ResponseStatusException(HttpStatus.NOT_IMPLEMENTED, "Coordinator config update is not implemented");
    }

    public Map<String, Object> initShards(Map<String, Object> params) {
        int numShards = positiveInteger(params, "num_shards", 8);
        int replicationFactor = positiveInteger(params, "replication_factor", 2);

        if (!coordinatorAdminClient.initShards(numShards, replicationFactor).getSuccess()) {
            throw new IllegalStateException("Coordinator failed to initialize shards");
        }

        return Map.of("success", true, "num_shards", numShards, "replication_factor", replicationFactor);
    }

    private static int positiveInteger(Map<String, Object> params, String field, int defaultValue) {
        Object value = params.getOrDefault(field, defaultValue);
        if (!(value instanceof Integer number) || number <= 0) {
            throw new IllegalArgumentException(field + " must be a positive 32-bit integer");
        }
        return number;
    }
}
