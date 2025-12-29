package com.danieljhkim.kvdb.kvadmin.client;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.Deadline;
import io.grpc.MethodDescriptor;

/**
 * gRPC client interceptor that enforces deadlines on all calls.
 */
public class GrpcClientInterceptor implements ClientInterceptor {

	private static final Logger logger = LoggerFactory.getLogger(GrpcClientInterceptor.class);

	private final long timeoutSeconds;

	public GrpcClientInterceptor(long timeoutSeconds) {
		this.timeoutSeconds = timeoutSeconds;
	}

	@Override
	public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
			MethodDescriptor<ReqT, RespT> method,
			CallOptions callOptions,
			Channel next) {

		// Apply deadline if not already set
		Deadline existingDeadline = callOptions.getDeadline();
		if (existingDeadline == null) {
			Deadline deadline = Deadline.after(timeoutSeconds, TimeUnit.SECONDS);
			callOptions = callOptions.withDeadline(deadline);
			logger.debug("Applied deadline of {}s to call: {}", timeoutSeconds, method.getFullMethodName());
		} else {
			logger.debug("Deadline already set for call: {}", method.getFullMethodName());
		}

		return next.newCall(method, callOptions);
	}

	/**
	 * Wraps a channel with this interceptor.
	 * 
	 * @param channel
	 *            The channel to wrap
	 * @return A new channel with the interceptor applied
	 */
	public Channel intercept(Channel channel) {
		return ClientInterceptors.intercept(channel, this);
	}
}
