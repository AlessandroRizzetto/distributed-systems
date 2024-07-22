package it.ds1;

import akka.actor.typed.ActorSystem;

public class Main {
    public static void main(String[] args) {
        ActorSystem<ReplicaActor.Command> system = ActorSystem.create(ReplicaActor.create(), "ReplicaSystem");
        // Sample usage of the system, this will be expanded as more functionality is built
        System.out.println("System started");
    }
}
