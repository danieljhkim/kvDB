package com.danieljhkim.kvdb.kvadmin.util;

/**
 * Time utilities.
 */
public class Time {

	/**
	 * Get current time in milliseconds.
	 */
	public static long nowMs() {
		return System.currentTimeMillis();
	}

	/**
	 * Get current time in seconds.
	 */
	public static long nowSeconds() {
		return System.currentTimeMillis() / 1000;
	}

	/**
	 * Convert milliseconds to seconds.
	 */
	public static long msToSeconds(long ms) {
		return ms / 1000;
	}

	/**
	 * Convert seconds to milliseconds.
	 */
	public static long secondsToMs(long seconds) {
		return seconds * 1000;
	}
}
