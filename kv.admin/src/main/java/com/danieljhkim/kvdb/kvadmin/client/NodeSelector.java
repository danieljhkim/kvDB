package com.danieljhkim.kvdb.kvadmin.client;

import java.util.List;
import java.util.Random;

import com.danieljhkim.kvdb.kvadmin.api.dto.NodeDto;

/**
 * Helper for selecting nodes for fanout operations.
 */
public class NodeSelector {

	private static final Random random = new Random();

	/**
	 * Select a random node from the list.
	 */
	public static NodeDto selectRandom(List<NodeDto> nodes) {
		if (nodes == null || nodes.isEmpty()) {
			return null;
		}
		return nodes.get(random.nextInt(nodes.size()));
	}

	/**
	 * Select the first alive node.
	 */
	public static NodeDto selectFirstAlive(List<NodeDto> nodes) {
		if (nodes == null || nodes.isEmpty()) {
			return null;
		}
		return nodes.stream()
				.filter(n -> "ALIVE".equals(n.getStatus()))
				.findFirst()
				.orElse(null);
	}

	/**
	 * Select all alive nodes.
	 */
	public static List<NodeDto> selectAllAlive(List<NodeDto> nodes) {
		if (nodes == null || nodes.isEmpty()) {
			return List.of();
		}
		return nodes.stream()
				.filter(n -> "ALIVE".equals(n.getStatus()))
				.toList();
	}
}

