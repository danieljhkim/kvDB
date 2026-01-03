package com.danieljhkim.kvdb.kvadmin.service;

import com.danieljhkim.kvdb.kvadmin.api.dto.TriggerRequestDto;
import com.danieljhkim.kvdb.kvadmin.client.NodeAdminClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Service for operational tasks (rebalance, compaction, etc.).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OpsService {

    private final NodeAdminClient nodeAdminClient;
    private final ShardAdminService shardAdminService;

    public TriggerRequestDto triggerRebalance(TriggerRequestDto request) {
        log.info("Triggering rebalance operation: {}", request);
        return shardAdminService.triggerRebalance(request);
    }

    public TriggerRequestDto triggerCompaction(TriggerRequestDto request) {
        log.info("Triggering compaction operation: {}", request);
        // TODO: Implement compaction logic
        // For each target node, call compaction RPC
        if (request.getTargetNodes() != null) {
            for (String nodeAddress : request.getTargetNodes()) {
                try {
                    nodeAdminClient.triggerCompaction(nodeAddress);
                } catch (Exception e) {
                    log.warn("Failed to trigger compaction on node: {}", nodeAddress, e);
                }
            }
        }
        return request;
    }

    public TriggerRequestDto triggerOperation(TriggerRequestDto request) {
        log.info("Triggering generic operation: {}", request);
        String operation = request.getOperation();
        return switch (operation.toUpperCase()) {
            case "REBALANCE" -> triggerRebalance(request);
            case "COMPACT" -> triggerCompaction(request);
            default -> {
                log.warn("Unknown operation: {}", operation);
                throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        };
    }
}
