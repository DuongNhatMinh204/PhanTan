package com.yrrhelp;

import com.yrrhelp.proto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Node extends KvStoreGrpc.KvStoreImplBase {
    private final NodeInfo nodeInfo;
    private final DataStore dataStore;
    private final List<NodeInfo> otherNodes;
    private final HeartbeatManager heartbeatManager;

    public Node(NodeInfo nodeInfo, String filePath, List<NodeInfo> otherNodes) {
        this.nodeInfo = nodeInfo;
        this.dataStore = new DataStore(filePath);
        this.otherNodes = otherNodes;
        this.heartbeatManager = new HeartbeatManager(nodeInfo, otherNodes);
        recoverData();
    }

    public void start() throws IOException, InterruptedException {
        Server server = ServerBuilder.forPort(nodeInfo.getPort())
                .addService(this)
                .build();
        server.start();
        System.out.println("Node " + nodeInfo.getId() + " started on port " + nodeInfo.getPort());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            System.out.println("Node " + nodeInfo.getId() + " stopped");
        }));
        server.awaitTermination();
    }

    @Override
    public void put(PutRequest req, StreamObserver<Response> responseObserver) {
        String key = req.getKey();
        String value = req.getValue();
        dataStore.put(key, value);
        syncToOtherNodes(key, value, false);
        responseObserver.onNext(Response.newBuilder().setStatus("OK").build());
        responseObserver.onCompleted();
    }

    @Override
    public void get(GetRequest req, StreamObserver<Response> responseObserver) {
        String key = req.getKey();
        String value = dataStore.get(key);
        if (value != null) {
            responseObserver.onNext(Response.newBuilder().setStatus("OK").setValue(value).build());
        } else {
            responseObserver.onNext(Response.newBuilder().setStatus("NOT_FOUND").build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest req, StreamObserver<Response> responseObserver) {
        String key = req.getKey();
        dataStore.delete(key);
        syncToOtherNodes(key, "", true);
        responseObserver.onNext(Response.newBuilder().setStatus("OK").build());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(HeartbeatRequest req, StreamObserver<Response> responseObserver) {
        heartbeatManager.updateHeartbeat(req.getNodeId());
        responseObserver.onNext(Response.newBuilder().setStatus("OK").build());
        responseObserver.onCompleted();
    }

    @Override
    public void sync(SyncRequest req, StreamObserver<Response> responseObserver) {
        if (req.getIsDelete()) {
            dataStore.delete(req.getKey());
        } else {
            dataStore.put(req.getKey(), req.getValue());
        }
        responseObserver.onNext(Response.newBuilder().setStatus("OK").build());
        responseObserver.onCompleted();
    }

    private void syncToOtherNodes(String key, String value, boolean isDelete) {
        for (NodeInfo node : otherNodes) {
            try {
                ManagedChannel channel = ManagedChannelBuilder.forAddress(node.getHost(), node.getPort())
                        .usePlaintext()
                        .build();
                KvStoreGrpc.KvStoreBlockingStub stub = KvStoreGrpc.newBlockingStub(channel);
                stub.sync(SyncRequest.newBuilder()
                        .setKey(key)
                        .setValue(value)
                        .setIsDelete(isDelete)
                        .build());
                channel.shutdown();
            } catch (Exception e) {
                System.out.println("Failed to sync to " + node.getId());
            }
        }
    }

    private void recoverData() {
        int maxRetries = 5;
        int retryDelayMs = 2000; // 2 giây thử lại 1 lần
        for (NodeInfo node : otherNodes) {
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(node.getHost(), node.getPort())
                            .usePlaintext()
                            .build();
                    KvStoreGrpc.KvStoreBlockingStub stub = KvStoreGrpc.newBlockingStub(channel);
                    GetOperationLogResponse resp = stub.getOperationLog(GetOperationLogRequest.newBuilder()
                            .setFromTimestamp(0) // Lấy tất cả log từ đầu
                            .build());
                    for (Operation op : resp.getOperationsList()) {
                        if (op.getIsDelete()) {
                            dataStore.delete(op.getKey());
                        } else {
                            dataStore.put(op.getKey(), op.getValue());
                        }
                    }
                    System.out.println("Has restored the status of the log of  " + node.getId());
                    return;
                } catch (Exception e) {
                    System.out.println("Error  " + attempt + " unsuccessfully " + node.getId() + ": " + e.getMessage());
                    if (attempt < maxRetries) {
                        try {
                            Thread.sleep(retryDelayMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                } finally {
                    if (channel != null) {
                        channel.shutdown();
                        try {
                            if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
                                channel.shutdownNow();
                            }
                        } catch (InterruptedException ex) {
                            channel.shutdownNow();
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        }
        System.out.println("Cannot restore the status from any nodes .");
    }

    @Override
    public void getOperationLog(GetOperationLogRequest req,StreamObserver<GetOperationLogResponse> responseObserver){
        List<Operation> ops = dataStore.getOperationLog(req.getFromTimestamp()) ;
        GetOperationLogResponse response = GetOperationLogResponse.newBuilder()
                .addAllOperations(ops)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        List<NodeInfo> nodes = Arrays.asList(
                new NodeInfo("node1", "localhost", 50051),
                new NodeInfo("node2", "localhost", 50052),
                new NodeInfo("node3", "localhost", 50053)
        );
        String id = args[0];
        NodeInfo currentNode = nodes.stream().filter(n -> n.getId().equals(id)).findFirst().orElseThrow();
        List<NodeInfo> otherNodes = nodes.stream().filter(n -> !n.getId().equals(id)).toList();
        Node node = new Node(currentNode, "src/main/resources/" + id + ".json", otherNodes);
        node.start();
    }
}