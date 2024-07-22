package it.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.ReceiveTimeout;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Coordinator extends AbstractActor {
    private static final int QUORUM_SIZE = 2;
    private final Map<EpochSequencePair, Set<ActorRef>> pendingAcks = new HashMap<>();
    private int currentEpoch = 0;
    private int sequenceNumber = 0;

    public static Props props() {
        return Props.create(Coordinator.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Replica.UpdateRequest.class, this::onUpdateRequest)
                .match(Replica.WriteOkMessage.class, this::onWriteOkMessage)
                .match(ReceiveTimeout.class, this::onReceiveTimeout)
                .build();
    }

    private void onUpdateRequest(Replica.UpdateRequest msg) {
        sequenceNumber++;
        EpochSequencePair pair = new EpochSequencePair(currentEpoch, sequenceNumber);
        System.out.println("Coordinator received update request from client " + msg.client + " with new value " + msg.newValue + " for pair " + pair);
        Replica.UpdateMessage updateMessage = new Replica.UpdateMessage(pair, msg.newValue);
        Set<ActorRef> acks = new HashSet<>();
        pendingAcks.put(pair, acks);
        for (ActorRef replica : getContext().getChildren()) {
            replica.tell(updateMessage, getSelf());
        }
        getContext().setReceiveTimeout(Duration.ofSeconds(5));
    }

    private void onWriteOkMessage(Replica.WriteOkMessage msg) {
        System.out.println("Coordinator received WriteOk message from replica " + getSender() + " for " + msg.pair);
        Set<ActorRef> acks = pendingAcks.get(msg.pair);
        if (acks != null) {
            acks.add(getSender());
            if (acks.size() >= QUORUM_SIZE) {
                System.out.println("Quorum reached for " + msg.pair);
                for (ActorRef replica : getContext().getChildren()) {
                    replica.tell(msg, getSelf());
                }
                pendingAcks.remove(msg.pair);
                getContext().setReceiveTimeout(Duration.ofSeconds(5));
            }
        }
    }

    private void onReceiveTimeout(ReceiveTimeout timeout) {
        System.out.println("Coordinator timeout while waiting for acks.");
        // Handle timeout for receiving acks (e.g., retry or fail the update)
    }
}
