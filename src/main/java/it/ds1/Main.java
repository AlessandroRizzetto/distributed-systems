package it.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

public class Main {
    private static final List<String> logMessages = new ArrayList<>();
    public static final ActorSystem system = ActorSystem.create("replica-system");
    private static ActorRef client;

    public static void customPrint(String message) {
        System.out.println(message);
        logMessages.add(message);
    }

    public static void delay(int d) {
        try {
            Thread.sleep(d);
        } catch (Exception ignored) {
        }
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
        final int N = 5; // Number of replicas
        ArrayList<ActorRef> replicas = new ArrayList<>();
        HashMap<Integer, ActorRef> replicasMap = new HashMap<>();

        ActorRef coordinator = system.actorOf(Props.create(Coordinator.class, N, new EpochSequencePair(0, 0)),
                "coordinator");
        for (int i = 0; i < N; i++) {
            ActorRef replica = system.actorOf(Props.create(Replica.class, i, coordinator), "replica" + i);
            replica.tell(new Replica.Register(i), ActorRef.noSender());
            replicas.add(replica);
            replicasMap.put(i, replica);
        }

        // All replicas should know about each other
        for (ActorRef replica : replicas) {
            replica.tell(new Replica.AddReplicaRequest(new HashMap<>(replicasMap)), coordinator);
        }

        client = system.actorOf(Props.create(Client.class, replicas, coordinator), "client");
        client.tell(new Client.WriteRequest(replicas.get(0), 100), ActorRef.noSender());
        delay(1000);
        // coordinator.tell(new Coordinator.Crash(0), ActorRef.noSender()); // Simulate
        // coordinator crash
        delay(20000);
        client.tell(new Client.ReadRequest(replicas.get(0)), ActorRef.noSender());
        // replicas.get(1).tell(new Replica.Crash(0), ActorRef.noSender()); // Simulate
        // replica crash
        // delay(1000);
        // client.tell(new Client.WriteRequest(replicas.get(0), 200), ActorRef.noSender());
        // delay(20000);
        // client.tell(new Client.WriteRequest(replicas.get(0), 3000),
        // ActorRef.noSender());

        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ignored) {
        }

        saveLogToFile("log.txt");
        system.terminate();
    }
}
