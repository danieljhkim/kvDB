package com.danieljhkim.kvdb.kvadmin.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Request body for key-to-shard routing diagnostics. The key is never echoed in logs or responses.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ResolveKeyRequestDto {

    @JsonProperty("key_base64")
    @ToString.Exclude
    private String keyBase64;
}
