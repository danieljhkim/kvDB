package com.danieljhkim.kvdb.kvadmin.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Error response DTO.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ErrorDto {

    private String error;
    private String message;
    private String code;
    private long timestampMs;

    // Optional fields for routing hints
    private String shardId;
    private String leaderHint;
    private String newNodeHint;
}
