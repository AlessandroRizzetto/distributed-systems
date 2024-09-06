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
        final int N = 5; // Number of replicas
        List<ActorRef> replicas = new ArrayList<>();

        ActorRef coordinator = system.actorOf(Props.create(Coordinator.class, N), "coordinator");
        for (int i = 0; i < N; i++) {
            ActorRef replica = system.actorOf(Props.create(Replica.class, i, coordinator), "replica" + i);
            replicas.add(replica);
        }

        ActorRef client = system.actorOf(Props.create(Client.class, replicas, coordinator), "client");
        client.tell(new Client.WriteRequest(replicas.get(0), 100), ActorRef.noSender());
        delay(1000);
        coordinator.tell(new Coordinator.Crash(0), ActorRef.noSender()); // Simulate coordinator crash
        client.tell(new Client.ReadRequest(replicas.get(0)), ActorRef.noSender());
        delay(5000);
        client.tell(new Client.WriteRequest(replicas.get(0), 200), ActorRef.noSender());

        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ignored) {}

        saveLogToFile("log.txt");
        system.terminate();
    }
}
