package com.danieljhkim.kvdb.kvadmin.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * JSON body for {@code POST /admin/nodes/{nodeId}/status}.
 *
 * <p>{@code status} is the coordinator node-status name. A raw string body is not accepted.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SetNodeStatusRequestDto {

    public static final String STATUS_PATTERN = "ALIVE|SUSPECT|DEAD";

    @NotBlank(message = "is required") @Pattern(regexp = STATUS_PATTERN, message = "must be ALIVE, SUSPECT, or DEAD") private String status;
}
