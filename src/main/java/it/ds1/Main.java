package it.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.HashSet;
import java.util.Set;

public class Main {

    public static void main(String[] args) {

        // Crea il sistema di attori
        ActorSystem system = ActorSystem.create("ReplicationSystem");

        // Crea il coordinatore
        Set<ActorRef> replicas = new HashSet<>();
        ActorRef coordinator = system.actorOf(Coordinator.props(replicas), "Coordinator");

        // Crea le repliche e assegna loro il coordinatore
        for (int i = 1; i <= 3; i++) {
            ActorRef replica = system.actorOf(Replica.props("Replica" + i, coordinator), "Replica" + i);
            replicas.add(replica);
        }

        // Simula richieste dei clienti
        ActorRef client = system.actorOf(Replica.props("Client1", coordinator), "Client1");
        ActorRef replica1 = replicas.iterator().next();

        replica1.tell(new Replica.ReadRequest(client), ActorRef.noSender());
        replica1.tell(new Replica.WriteRequest(client, 42), ActorRef.noSender());

        try {
            // Attendi prima di terminare il sistema per vedere l'output
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        system.terminate(); // Arresta il sistema di attori
    }
}
