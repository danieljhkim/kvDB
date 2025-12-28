package com.danieljhkim.kvdb.kvclustercoordinator.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable record representing a storage node's metadata.
 * Used by the coordinator to track node membership and health.
 */
public record NodeRecord(String nodeId,String address,String zone,String rack,NodeStatus status,long lastHeartbeatMs,Map<String,String>capacityHints){

public enum NodeStatus {
	UNSPECIFIED, ALIVE, SUSPECT, DEAD

	}

	/**
	 * Canonical constructor with defensive copy of capacityHints.
	 */
	public NodeRecord{if(nodeId==null||nodeId.isBlank()){throw new IllegalArgumentException("nodeId cannot be null or blank");}if(address==null||address.isBlank()){throw new IllegalArgumentException("address cannot be null or blank");}if(status==null){status=NodeStatus.UNSPECIFIED;}capacityHints=capacityHints==null?Collections.emptyMap():Collections.unmodifiableMap(new HashMap<>(capacityHints));}

	/**
	 * Convenience constructor for minimal node registration.
	 */
	public static NodeRecord create(String nodeId, String address, String zone) {
		return new NodeRecord(
				nodeId,
				address,
				zone,
				null,
				NodeStatus.ALIVE,
				System.currentTimeMillis(),
				Collections.emptyMap());
	}

	/**
	 * Returns a new NodeRecord with updated status.
	 */
	public NodeRecord withStatus(NodeStatus newStatus) {
		return new NodeRecord(
				nodeId, address, zone, rack, newStatus, lastHeartbeatMs, capacityHints);
	}

	/**
	 * Returns a new NodeRecord with updated heartbeat timestamp.
	 */
	public NodeRecord withHeartbeat(long newHeartbeatMs) {
		return new NodeRecord(nodeId, address, zone, rack, status, newHeartbeatMs, capacityHints);
	}

	/**
	 * Returns a new NodeRecord with updated heartbeat and marked as ALIVE.
	 */
	public NodeRecord withHeartbeatAlive(long newHeartbeatMs) {
		return new NodeRecord(
				nodeId, address, zone, rack, NodeStatus.ALIVE, newHeartbeatMs, capacityHints);
	}
}
