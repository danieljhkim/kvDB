package com.danieljhkim.kvdb.kvadmin.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Key range for a shard.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KeyRangeDto {

	private byte[] startKey; // inclusive
	private byte[] endKey; // exclusive
}

