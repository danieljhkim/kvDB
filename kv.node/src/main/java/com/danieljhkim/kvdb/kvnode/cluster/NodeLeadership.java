package com.danieljhkim.kvdb.kvnode.cluster;

import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;

/**
 * Basic leadership helper for storage nodes.
 *
 * <p>
 * This milestone uses a temporary single write leader model:
 * only the configured write leader accepts writes (Set/Delete).
 */
public class NodeLeadership {

	private final String nodeId;
	private final String writeLeaderId;
	private final String writeLeaderHost;
	private final int writeLeaderPort;

	public NodeLeadership(SystemConfig config) {
		this(
				config.getProperty("kvdb.node.id", ""),
				config.getProperty("kvdb.node.writeLeaderId", "node-1"),
				config.getProperty("kvdb.node.writeLeaderHost", "localhost"),
				Integer.parseInt(config.getProperty("kvdb.node.writeLeaderPort", "8001")));
	}

	public NodeLeadership(
			String nodeId,
			String writeLeaderId,
			String writeLeaderHost,
			int writeLeaderPort) {
		this.nodeId = nodeId;
		this.writeLeaderId = writeLeaderId;
		this.writeLeaderHost = writeLeaderHost;
		this.writeLeaderPort = writeLeaderPort;
	}

	public boolean isWriteLeader() {
		return nodeId != null && !nodeId.isEmpty() && nodeId.equals(writeLeaderId);
	}

	public String getWriteLeaderHint() {
		return writeLeaderHost + ":" + writeLeaderPort;
	}
}
