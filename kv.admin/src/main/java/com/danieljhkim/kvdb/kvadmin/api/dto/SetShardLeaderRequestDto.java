package com.danieljhkim.kvdb.kvadmin.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * JSON body for {@code POST /admin/shards/{shardId}/leader}.
 *
 * <p>Wire field is {@code leader_node_id}. A raw string body is not accepted.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SetShardLeaderRequestDto {

    @NotBlank(message = "is required") @JsonProperty("leader_node_id")
    private String leaderNodeId;
}
