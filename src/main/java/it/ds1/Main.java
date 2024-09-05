package it.ds1;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    private static final List<String> logMessages = new ArrayList<>();

    public static void customPrint(String message) {
        System.out.println(message);
        logMessages.add(message);
    }

    public static void delay(int d) {
        try {
            Thread.sleep(d);
        } catch (Exception ignored) {}
    }

    private static void saveLogToFile(String fileName) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (String message : logMessages) {
                writer.write(message);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("replica-system");
        final int N = 5;  // Number of replicas
        List<ActorRef> replicas = new ArrayList<>();

        // Create coordinator 
        ActorRef coordinator = system.actorOf(Props.create(Coordinator.class, N), "coordinator");

        // Create replicas
        for (int i = 0; i < N; i++) {
            ActorRef replica = system.actorOf(Props.create(Replica.class, i, coordinator), "replica" + i);
            replicas.add(replica);
        }

        // Simulate client read and write requests
        ActorRef client = system.actorOf(Props.create(Client.class, replicas, coordinator), "client");

        // Sample write and read operations
        client.tell(new Client.WriteRequest(replicas.get(0), 100), ActorRef.noSender());
        delay(10000);
        client.tell(new Client.ReadRequest(replicas.get(1)), ActorRef.noSender());
        // client.tell(new Client.WriteRequest(replicas.get(0), 200), ActorRef.noSender());

        // Print all the replicas' values
        // for (ActorRef replica : replicas) {
        //     client.tell(new Client.ReadRequest(replica), ActorRef.noSender());
        // }

        // Ensure shutdown
        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
          } 
          catch (IOException ignored) {}

        saveLogToFile("log.txt");
        system.terminate();
    }
}
