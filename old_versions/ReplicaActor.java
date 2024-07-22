package it.ds1;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

import java.util.HashMap;
import java.util.Map;

public class ReplicaActor extends AbstractBehavior<ReplicaActor.Command> {
    interface Command {}

    static class WriteRequest implements Command {
        final String value;
        final UpdateId updateId;

        WriteRequest(String value, UpdateId updateId) {
            this.value = value;
            this.updateId = updateId;
        }
    }

    static class UpdateId {
        final int epoch;
        final int sequenceNumber;

        UpdateId(int epoch, int sequenceNumber) {
            this.epoch = epoch;
            this.sequenceNumber = sequenceNumber;
        }
    }

    private final Map<UpdateId, String> updates = new HashMap<>();

    private ReplicaActor(ActorContext<Command> context) {
        super(context);
    }

    static Behavior<Command> create() {
        return Behaviors.setup(ReplicaActor::new);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(WriteRequest.class, this::onWriteRequest)
            .build();
    }

    private Behavior<Command> onWriteRequest(WriteRequest request) {
        updates.put(request.updateId, request.value);
        getContext().getLog().info("Update Received - Epoch: {}, Sequence: {}, Value: {}",
                                    request.updateId.epoch, request.updateId.sequenceNumber, request.value);
        return this;
    }
}
