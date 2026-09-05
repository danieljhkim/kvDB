package com.danieljhkim.kvdb.kvadmin.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Coordinator placement for a key at observation time. This is not proof that a value exists and
 * does not reflect gateway cache state.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KeyPlacementDto {

    @JsonProperty("shard_id")
    private String shardId;

    private long epoch;
    private List<String> replicas;
    private String leader;

    @JsonProperty("config_state")
    private String configState;
}
