package com.danieljhkim.kvdb.kvadmin.api.dto;

import jakarta.validation.constraints.NotBlank;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request to trigger an operation (rebalance, compaction, etc.).
 *
 * <p>{@code operation} is required only for {@code POST /admin/ops/trigger}
 * ({@link OnTrigger}). Rebalance and compact endpoints imply the operation.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TriggerRequestDto {

    /** Validation group for {@code POST /admin/ops/trigger}. */
    public interface OnTrigger {}

    @NotBlank(groups = OnTrigger.class, message = "is required") private String operation; // REBALANCE, COMPACT, etc.

    private Map<String, String> parameters;
    private List<String> targetShards; // optional: specific shards
    private List<String> targetNodes; // optional: specific nodes
}
