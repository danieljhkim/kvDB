package com.danieljhkim.kvdb.kvclustercoordinator.cluster;

import com.kvdb.proto.kvstore.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public class GrpcClusterNodeClient implements ClusterNodeClient {

	private final ManagedChannel channel;
	private final KVServiceGrpc.KVServiceBlockingStub stub;

	public GrpcClusterNodeClient(String host, int port) {
		// Explicitly use the dns scheme to bypass the resolver selection issue
		io.grpc.internal.DnsNameResolverProvider provider = new io.grpc.internal.DnsNameResolverProvider();
		io.grpc.NameResolverRegistry.getDefaultRegistry().register(provider);

		this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
		this.stub = KVServiceGrpc.newBlockingStub(channel);
	}

	@Override
	public String sendGet(String key) {
		KeyRequest request = KeyRequest.newBuilder().setKey(key).build();
		ValueResponse response = stub.get(request);
		return response.getValue();
	}

	@Override
	public boolean sendSet(String key, String value) {
		KeyValueRequest request = KeyValueRequest.newBuilder().setKey(key).setValue(value).build();
		SetResponse response = stub.set(request);
		return response.getSuccess();
	}

	@Override
	public boolean sendDelete(String key) {
		DeleteRequest request = DeleteRequest.newBuilder().setKey(key).build();
		DeleteResponse response = stub.delete(request);
		return response.getSuccess();
	}

	@Override
	public boolean ping() {
		PingRequest request = PingRequest.newBuilder().build();
		PingResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS).ping(request);
		return "pong".equalsIgnoreCase(response.getMessage());
	}

	@Override
	public String shutdown() {
		ShutdownRequest request = ShutdownRequest.newBuilder().build();
		ShutdownResponse response = stub.shutdown(request);
		channel.shutdown();
		return response.getMessage();
	}
}
