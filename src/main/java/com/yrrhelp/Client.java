package com.yrrhelp;
import com.yrrhelp.proto.KvStoreGrpc;
import com.yrrhelp.proto.PutRequest;
import com.yrrhelp.proto.GetRequest;
import com.yrrhelp.proto.DeleteRequest;
import com.yrrhelp.proto.Response;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Scanner;

public class Client {
    private final KvStoreGrpc.KvStoreBlockingStub stub;

    public Client(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.stub = KvStoreGrpc.newBlockingStub(channel);
    }

    public String put(String key, String value) {
        Response response = stub.put(PutRequest.newBuilder().setKey(key).setValue(value).build());
        return response.getStatus();
    }

    public String get(String key) {
        Response response = stub.get(GetRequest.newBuilder().setKey(key).build());
        return response.getStatus().equals("OK") ? response.getValue() : "NOT_FOUND";
    }

    public String delete(String key) {
        Response response = stub.delete(DeleteRequest.newBuilder().setKey(key).build());
        return response.getStatus();
    }

    public static void main(String[] args) {
        Client client = new Client("localhost", 50051);
//        Client client2 = new Client("localhost", 50052);
//        Client client3 = new Client("localhost", 50053);
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("Enter command (PUT/GET/DELETE/EXIT): ");
            String command = scanner.nextLine().trim().toUpperCase();

            if (command.equals("EXIT")) {
                break;
            }

            switch (command) {
                case "PUT":
                    System.out.println("Enter key: ");
                    String key = scanner.nextLine().trim();
                    System.out.println("Enter value: ");
                    String value = scanner.nextLine().trim();
                    client.put(key, value);
                    break;
                case "GET":
                    System.out.println("Enter key: ");
                    key = scanner.nextLine().trim();
                    String valueGet = client.get(key);
                    System.out.println("Value: " + valueGet);
                    break;
                case "DELETE":
                    System.out.println("Enter key: ");
                    key = scanner.nextLine().trim();
                    System.out.println("Deleting key: " + key); // debug
                    client.delete(key);
                    System.out.println("Delete done"); // debug
                    break;
                default:
                    System.out.println("Invalid command");
            }
        }
        scanner.close();
    }
}
