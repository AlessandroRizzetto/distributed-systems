package it.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// Coordinator class that handles updates and manages the replicas
public class Coordinator extends AbstractActor {

    private int epoch = 0; // Current epoch
    private int sequenceNumber = 0; // Current sequence number
    private final Set<ActorRef> replicas; // Set of replicas in the system
    private final Map<EpochSequencePair, Set<ActorRef>> pendingAcks; // Track pending ACKs

    // Constructor to initialize the set of replicas
    public Coordinator(Set<ActorRef> replicas) {
        this.replicas = replicas;
        this.pendingAcks = new HashMap<>();
    }

    // Factory method to create a Coordinator actor
    public static Props props(Set<ActorRef> replicas) {
        return Props.create(Coordinator.class, replicas);
    }

    // Message classes for communication between actors
    public static class WriteRequest implements Serializable {
        public final ActorRef client;
        public final int newValue;

        public WriteRequest(ActorRef client, int newValue) {
            this.client = client;
            this.newValue = newValue;
        }
    }

    public static class ReadResponse implements Serializable {
        public final int value;

        public ReadResponse(int value) {
            this.value = value;
        }
    }

    public static class WriteOk implements Serializable {
        public final EpochSequencePair epochSequencePair;

        public WriteOk(EpochSequencePair epochSequencePair) {
            this.epochSequencePair = epochSequencePair;
        }
    }

    // Method to handle incoming messages
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(WriteRequest.class, this::onWriteRequest)
                .match(Replica.Ack.class, this::onAck)
                .build();
    }

    // Handle write requests from replicas
    private void onWriteRequest(WriteRequest request) {
        System.out.println("Coordinator received write request from Replica with value: " + request.newValue);
        sequenceNumber++;
        EpochSequencePair pair = new EpochSequencePair(epoch, sequenceNumber);
        pendingAcks.put(pair, new HashSet<>(replicas));
        for (ActorRef replica : replicas) {
            replica.tell(new Replica.Update(pair, request.newValue), getSelf());
        }
    }

    // Handle ACK messages from replicas
    private void onAck(Replica.Ack ack) {
        System.out.println("Coordinator received ACK for: " + ack.epochSequencePair);
        Set<ActorRef> acks = pendingAcks.get(ack.epochSequencePair);
        if (acks != null) {
            acks.remove(getSender());
            if (acks.size() <= replicas.size() / 2) { // Check if a quorum is reached
                for (ActorRef replica : replicas) {
                    replica.tell(new WriteOk(ack.epochSequencePair), getSelf());
                }
                pendingAcks.remove(ack.epochSequencePair); // Remove the entry as it's now completed
            }
        }
    }
}

