package com.danieljhkim.kvdb.kvclustercoordinator.protocol;

import com.danieljhkim.kvdb.kvclustercoordinator.cluster.ClusterNodeClient;
import com.danieljhkim.kvdb.kvcommon.protocol.CommandExecutor;

import lombok.Setter;

/** Adapter that wraps client (grpc or http) calls to a cluster node. */
@Setter
public class ClusterCommandExecutor implements CommandExecutor {

	private ClusterNodeClient client;

	public ClusterCommandExecutor(ClusterNodeClient client) {
		this.client = client;
	}

	public ClusterCommandExecutor() {
	}

	@Override
	public String get(String key) {
		if (client == null) {
			throw new IllegalStateException("ClusterNodeClient is not set");
		}
		return client.sendGet(key);
	}

	@Override
	public boolean put(String key, String value) {
		if (client == null) {
			throw new IllegalStateException("ClusterNodeClient is not set");
		}
		return client.sendSet(key, value);
	}

	@Override
	public boolean delete(String key) {
		if (client == null) {
			throw new IllegalStateException("ClusterNodeClient is not set");
		}
		return client.sendDelete(key);
	}

	@Override
	public String shutdown() {
		if (client == null) {
			throw new IllegalStateException("ClusterNodeClient is not set");
		}
		return client.shutdown();
	}
}
