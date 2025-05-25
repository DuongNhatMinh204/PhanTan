package com.yrrhelp;

import com.yrrhelp.proto.KvStoreGrpc;
import com.yrrhelp.proto.HeartbeatRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HeartbeatManager {
    private final NodeInfo currentNode;
    private final List<NodeInfo> otherNodes;
    private final Map<String, Long> lastHeartbeat = new HashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public HeartbeatManager(NodeInfo currentNode, List<NodeInfo> otherNodes) {
        this.currentNode = currentNode;
        this.otherNodes = otherNodes;
//        startHeartbeat(); // gửi heart beat đến các node khác
//        startMonitoring(); // theo dõi
        scheduler.schedule(() -> {
            startHeartbeat();
            startMonitoring();
        }, 5, TimeUnit.SECONDS);
    }

    // 5 giây gửi heart beat 1 lần
    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(() -> {
            for (NodeInfo node : otherNodes) {
                ManagedChannel channel = null;
                try {
                    // tạo 1 kết nối grpc tới node đích
                    System.out.println("Try connect to " + node.getHost() + ":" + node.getPort());
                    channel = ManagedChannelBuilder.forAddress(node.getHost(), node.getPort())
                            .usePlaintext()
                            .build();
                    KvStoreGrpc.KvStoreBlockingStub stub = KvStoreGrpc.newBlockingStub(channel);

                    stub.heartbeat(HeartbeatRequest.newBuilder().setNodeId(currentNode.getId()).build());
                    System.out.println("Send signals successful to   " + node.getHost() + ":" + node.getPort());
                } catch (Exception e) {
                    System.out.println("Cannot send signals to " + node.getId() + ": " + e.getMessage());
                } finally {
                    if (channel != null){
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
        }, 0, 5, TimeUnit.SECONDS);
    }

    private void startMonitoring() {
        scheduler.scheduleAtFixedRate(() -> {
            long currentTime = System.currentTimeMillis();
            for (NodeInfo node : otherNodes) {
                lastHeartbeat.computeIfAbsent(node.getId(), k -> currentTime);
                if (currentTime - lastHeartbeat.get(node.getId()) > 10000) {
                    System.out.println("Node " + node.getId() + " is down!");
                }else{
                    System.out.println("Node " + node.getId() + " is up!");
                }
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    public void updateHeartbeat(String nodeId) {
        lastHeartbeat.put(nodeId, System.currentTimeMillis());
    }
}