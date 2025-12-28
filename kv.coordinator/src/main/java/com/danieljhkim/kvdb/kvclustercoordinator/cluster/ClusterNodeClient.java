package com.danieljhkim.kvdb.kvclustercoordinator.cluster;

public interface ClusterNodeClient {
	boolean sendSet(String key, String value);

	boolean sendDelete(String key);

	String sendGet(String key);

	boolean ping();

	String shutdown();
}
