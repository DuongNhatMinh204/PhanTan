package com.yrrhelp;

import com.yrrhelp.proto.KvStoreGrpc;
import com.yrrhelp.proto.PutRequest;
import com.yrrhelp.proto.GetRequest;
import com.yrrhelp.proto.DeleteRequest;
import com.yrrhelp.proto.HeartbeatRequest;
import com.yrrhelp.proto.SyncRequest;
import com.yrrhelp.proto.Response;
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
        for (NodeInfo node : otherNodes) {
            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder.forAddress(node.getHost(), node.getPort())
                        .usePlaintext()
                        .build();
                KvStoreGrpc.KvStoreBlockingStub stub = KvStoreGrpc.newBlockingStub(channel);

                Response resp = stub.get(GetRequest.newBuilder().setKey("snapshot").build());
                if (resp.getStatus().equals("OK")) {
                    dataStore.put("snapshot", resp.getValue());
                    System.out.println("Recovered snapshot from " + node.getId());
                    break; // Đã recover thành công thì dừng lại
                }
            } catch (Exception e) {
                System.out.println("Failed to recover from " + node.getId() + ": " + e.getMessage());
            } finally {
                if (channel != null) {
                    channel.shutdown();
                    try {
                        if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
                            channel.shutdownNow();
                        }
                    } catch (InterruptedException ex) {
                        channel.shutdownNow();
                    }
                }
            }
        }
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