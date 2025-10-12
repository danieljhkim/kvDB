package com.kvdb.kvclustercoordinator.protocol;

import com.kvdb.kvclustercoordinator.cluster.ClusterNodeClient;
import com.kvdb.kvcommon.protocol.CommandExecutor;


/**
 * Adapter that wraps client (grpc or http) calls to a cluster node.
 */
public class ClusterCommandExecutor implements CommandExecutor {
    
    private final ClusterNodeClient client;
    
    public ClusterCommandExecutor(ClusterNodeClient client) {
        this.client = client;
    }
    
    @Override
    public String get(String key) {
        return client.sendGet(key);
    }
    
    @Override
    public boolean put(String key, String value) {
        return client.sendSet(key, value);
    }
    
    @Override
    public boolean delete(String key) {
        return client.sendDelete(key);
    }
}
