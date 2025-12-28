package com.danieljhkim.kvdb.kvclustercoordinator.protocol;

import com.danieljhkim.kvdb.kvcommon.protocol.CommandExecutor;

import java.util.HashMap;
import java.util.Map;

/** Fake CommandExecutor for testing purposes */
public class FakeCommandExecutor implements CommandExecutor {

	private final Map<String, String> store = new HashMap<>();

	@Override
	public String get(String key) {
		return store.get(key);
	}

	@Override
	public boolean put(String key, String value) {
		store.put(key, value);
		return true;
	}

	@Override
	public boolean delete(String key) {
		return store.remove(key) != null;
	}

	@Override
	public boolean exists(String key) {
		return store.containsKey(key);
	}
}
