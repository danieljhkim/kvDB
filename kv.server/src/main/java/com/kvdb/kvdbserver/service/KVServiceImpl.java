package com.kvdb.kvdbserver.service;

import com.kvdb.kvdbserver.repository.BaseRepository;
import com.kvdb.proto.kvstore.*;

import io.grpc.stub.StreamObserver;

public class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase {

    BaseRepository store;

    public KVServiceImpl(BaseRepository store) {
        this.store = store;
    }

    @Override
    public void get(KeyRequest request, StreamObserver<ValueResponse> responseObserver) {
        String key = request.getKey();
        String value = store.get(key);

        ValueResponse response =
                ValueResponse.newBuilder().setValue(value != null ? value : "").build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void set(KeyValueRequest request, StreamObserver<SetResponse> responseObserver) {
        store.update(request.getKey(), request.getValue());

        SetResponse response = SetResponse.newBuilder().setSuccess(true).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        boolean success = store.delete(request.getKey());

        DeleteResponse response = DeleteResponse.newBuilder().setSuccess(success).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
        PingResponse response = PingResponse.newBuilder().setMessage("pong").build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void shutdown(
            ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {
        ShutdownResponse response = ShutdownResponse.newBuilder().setMessage("goodbye").build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
        System.exit(0);
    }
}
