package com.example.distributed;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

import java.util.List;

public class CoordinatorActor extends AbstractBehavior<CoordinatorActor.Command> {

    public interface Command {}

    public static class UpdateRequest implements Command {
        final String newValue;
        final List<ActorRef<ReplicaActor.Command>> replicas;

        public UpdateRequest(String newValue, List<ActorRef<ReplicaActor.Command>> replicas) {
            this.newValue = newValue;
            this.replicas = replicas;
        }
    }

    public static class UpdateAck implements Command {
        final ActorRef<ReplicaActor.Command> replica;

        public UpdateAck(ActorRef<ReplicaActor.Command> replica) {
            this.replica = replica;
        }
    }

    private CoordinatorActor(ActorContext<Command> context) {
        super(context);
    }

    public static Behavior<Command> create() {
        return Behaviors.setup(CoordinatorActor::new);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(UpdateRequest.class, this::onUpdateRequest)
            .onMessage(UpdateAck.class, this::onUpdateAck)
            .build();
    }

    private Behavior<Command> onUpdateRequest(UpdateRequest command) {
        // Send update message to all replicas
        command.replicas.forEach(replica -> 
            replica.tell(new ReplicaActor.UpdateMessage(command.newValue, getContext().getSelf()))
        );
        return this;
    }

    private Behavior<Command> onUpdateAck(UpdateAck ack) {
        getContext().getLog().info("Received ACK from {}", ack.replica);
        return this;
    }
}
