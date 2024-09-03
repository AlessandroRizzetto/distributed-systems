package it.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("ActorSystem");

        // Create coordinator
        ActorRef coordinator = system.actorOf(Coordinator.props(), "coordinator");

        // Create replicas
        ActorRef replica1 = system.actorOf(Replica.props(coordinator), "replica1");
        ActorRef replica2 = system.actorOf(Replica.props(coordinator), "replica2");

        // Example usage
        replica1.tell(new Replica.UpdateRequest(coordinator, 10), ActorRef.noSender());
        replica2.tell(new Replica.UpdateRequest(coordinator, 20), ActorRef.noSender());
        replica1.tell(new Replica.UpdateRequest(coordinator, 30), ActorRef.noSender());
        replica2.tell(new Replica.UpdateRequest(coordinator, 40), ActorRef.noSender());
        

        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } 
        catch (IOException ignored) {}
        system.terminate();
    }
}
