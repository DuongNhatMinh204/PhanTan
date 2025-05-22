//package com.yrrhelp;
//
//import com.yrrhelp.keyvalue.KeyValueNode;
//import com.yrrhelp.keyvalue.NodeInfo;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.List;
//
//
//public class Main {
//    public static void main(String[] args) throws IOException, InterruptedException, IOException {
//        if (args.length != 1) {
//            System.err.println("Usage: java Main <nodeId>");
//            System.exit(1);
//        }
//
//        int nodeId = Integer.parseInt(args[0]);
//
//        List<NodeInfo> allNodes = Arrays.asList(
//                new NodeInfo(0, 8080),
//                new NodeInfo(1, 8081),
//                new NodeInfo(2, 8082)
//        );
//
//        int port = allNodes.get(nodeId).getPort();
//        KeyValueNode node = new KeyValueNode(nodeId, port, allNodes);
//        node.start();
//    }
//}
