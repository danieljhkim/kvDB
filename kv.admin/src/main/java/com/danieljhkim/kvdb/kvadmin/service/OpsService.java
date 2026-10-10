package com.danieljhkim.kvdb.kvadmin.service;

import com.danieljhkim.kvdb.kvadmin.api.dto.TriggerRequestDto;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Service for operational tasks such as rebalance.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OpsService {

    private final ShardAdminService shardAdminService;

    public TriggerRequestDto triggerRebalance(TriggerRequestDto request) {
        return shardAdminService.triggerRebalance(request);
    }

    public TriggerRequestDto triggerCompaction() {
        log.warn("Compaction requested but the node compaction RPC is not implemented");
        throw new ResponseStatusException(HttpStatus.NOT_IMPLEMENTED, "Node compaction RPC is not implemented");
    }

    public TriggerRequestDto triggerOperation(TriggerRequestDto request) {
        if (request == null
                || request.getOperation() == null
                || request.getOperation().isBlank()) {
            throw new IllegalArgumentException("operation is required");
        }
        String operation = request.getOperation();
        log.info("Triggering generic operation: {}", operation);
        return switch (operation.toUpperCase(Locale.ROOT)) {
            case "REBALANCE" -> triggerRebalance(request);
            case "COMPACT" -> triggerCompaction();
            default -> {
                log.warn("Unknown operation: {}", operation);
                throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        };
    }
}
