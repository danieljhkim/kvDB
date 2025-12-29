package com.danieljhkim.kvdb.kvadmin.util;

/**
 * Validation utilities.
 */
public class Validation {

	/**
	 * Validate that a string is not null or empty.
	 */
	public static void requireNonEmpty(String value, String fieldName) {
		if (value == null || value.trim().isEmpty()) {
			throw new IllegalArgumentException(fieldName + " cannot be null or empty");
		}
	}

	/**
	 * Validate that a value is not null.
	 */
	public static void requireNonNull(Object value, String fieldName) {
		if (value == null) {
			throw new IllegalArgumentException(fieldName + " cannot be null");
		}
	}

	/**
	 * Validate that a number is positive.
	 */
	public static void requirePositive(int value, String fieldName) {
		if (value <= 0) {
			throw new IllegalArgumentException(fieldName + " must be positive");
		}
	}

	/**
	 * Validate node address format (host:port).
	 */
	public static void validateNodeAddress(String address) {
		requireNonEmpty(address, "address");
		String[] parts = address.split(":");
		if (parts.length != 2) {
			throw new IllegalArgumentException("Invalid node address format: " + address);
		}
		try {
			int port = Integer.parseInt(parts[1]);
			if (port <= 0 || port > 65535) {
				throw new IllegalArgumentException("Invalid port: " + port);
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid port in address: " + address);
		}
	}
}

