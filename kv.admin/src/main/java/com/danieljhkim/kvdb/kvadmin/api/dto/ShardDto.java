package com.danieljhkim.kvdb.kvadmin.api.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Shard information.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShardDto {

    private String shardId;
    private long epoch;
    private List<String> replicas; // node IDs
    private String leader; // node ID
    private String configState; // STABLE, MOVING
    private KeyRangeDto keyRange;
}
