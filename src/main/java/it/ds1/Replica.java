package it.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;

public class Replica extends AbstractActor {
    private int value = 0; // initial value
    private final Map<EpochSequencePair, Integer> updateHistory = new HashMap<>();
    private final ActorRef coordinator;

    public static class ReadRequest {
        public final ActorRef client;
        public ReadRequest(ActorRef client) {
            this.client = client;
        }
    }

    public static class UpdateRequest {
        public final ActorRef client;
        public final int newValue;
        public UpdateRequest(ActorRef client, int newValue) {
            this.client = client;
            this.newValue = newValue;
        }
    }

    public static class UpdateMessage {
        public final EpochSequencePair pair;
        public final int newValue;
        public UpdateMessage(EpochSequencePair pair, int newValue) {
            this.pair = pair;
            this.newValue = newValue;
        }
    }

    public static class WriteOkMessage {
        public final EpochSequencePair pair;
        public WriteOkMessage(EpochSequencePair pair) {
            this.pair = pair;
        }
    }

    public static Props props(ActorRef coordinator) {
        return Props.create(Replica.class, () -> new Replica(coordinator));
    }

    public Replica(ActorRef coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadRequest.class, this::onReadRequest)
                .match(UpdateRequest.class, this::onUpdateRequest)
                .match(UpdateMessage.class, this::onUpdateMessage)
                .match(WriteOkMessage.class, this::onWriteOkMessage)
                .build();
    }

    private void onReadRequest(ReadRequest msg) {
        System.out.println("Client " + msg.client + " read req to " + getSelf());
        msg.client.tell(value, getSelf());
    }

    private void onUpdateRequest(UpdateRequest msg) {
        System.out.println("Client " + msg.client + " update req with new value " + msg.newValue);
        // Forward the request to the Coordinator
        coordinator.tell(msg, getSelf());
    }

    private void onUpdateMessage(UpdateMessage msg) {
        System.out.println("Received update message " + msg.pair);
        updateHistory.put(msg.pair, msg.newValue);
        getSender().tell(new WriteOkMessage(msg.pair), getSelf());
        System.out.println("Sent WriteOk message to the coordinator" + msg.pair);
    }

    private void onWriteOkMessage(WriteOkMessage msg) {
        System.out.println("Received WriteOk message " + msg.pair);
        value = updateHistory.get(msg.pair);
        System.out.println("Replica " + getSelf() + " update " + msg.pair.getEpoch() + ":" + msg.pair.getSequenceNumber() + " " + value);
    }
}
