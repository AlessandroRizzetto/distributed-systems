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

        // Create all replicas and make shure they know about the coordinator
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

        // Client that sends the requests
        client = system.actorOf(Props.create(Client.class, replicas, coordinator), "client");

        // =================================Tests================================

        // testReplicaCrash(replicas);
        // testWriteAndCoordinatorCrash(replicas, coordinator);
        // testCoordinatorCrash(coordinator);

        // Coordinator crash before sending WRITEOK -> on Coordinator set to true the
        // boolean testAckCrash
        // testWriteCoordinatorCrashPendingUpdateRead(replicas);

        // Replica crash during election -> on Replica set to true the boolean
        // isNextReplicaCrashedTest
        // testReplicaCrashDuringElection(replicas, coordinator);

        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ignored) {
        }

        saveLogToFile("log.txt");
        system.terminate();
    }

    // Write request -> read request -> replica crash -> write request
    private static void testReplicaCrash(ArrayList<ActorRef> replicas) {
        client.tell(new Client.WriteRequest(replicas.get(0), 100), ActorRef.noSender());
        delay(1000);
        client.tell(new Client.ReadRequest(replicas.get(0)), ActorRef.noSender());
        delay(1000);
        replicas.get(1).tell(new Replica.Crash(0), ActorRef.noSender()); // Simulate
        delay(1000);
        client.tell(new Client.WriteRequest(replicas.get(0), 200), ActorRef.noSender());
    }

    // Write request -> coordinator crash
    private static void testWriteAndCoordinatorCrash(ArrayList<ActorRef> replicas, ActorRef coordinator) {
        client.tell(new Client.WriteRequest(replicas.get(0), 100), ActorRef.noSender());
        delay(1000);
        client.tell(new Client.WriteRequest(replicas.get(0), 200), ActorRef.noSender());
        delay(1000);
        coordinator.tell(new Coordinator.Crash(0), ActorRef.noSender()); // Simulate
    }

    // Coordinator crash
    private static void testCoordinatorCrash(ActorRef coordinator) {
        coordinator.tell(new Coordinator.Crash(0), ActorRef.noSender()); // Simulate
    }

    // Write request -> coordinator crash -> recover update -> read request
    private static void testWriteCoordinatorCrashPendingUpdateRead(ArrayList<ActorRef> replicas) {
        client.tell(new Client.WriteRequest(replicas.get(0), 100), ActorRef.noSender());
        delay(20000);
        client.tell(new Client.ReadRequest(replicas.get(0)), ActorRef.noSender());
    }

    private static void testReplicaCrashDuringElection(ArrayList<ActorRef> replicas, ActorRef coordinator) {
        coordinator.tell(new Coordinator.Crash(0), ActorRef.noSender()); // Simulate
        delay(5000);
        client.tell(new Client.WriteRequest(replicas.get(0), 100), ActorRef.noSender());
    }
}
