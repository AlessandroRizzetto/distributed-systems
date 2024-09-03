package it.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

// Replica class representing each node in the system
public class Replica extends AbstractActor {

    private final String replicaID; // Unique ID for each replica
    private int value = 0; // Local value stored by the replica
    private final ActorRef coordinator; // Reference to the coordinator actor
    private final Map<EpochSequencePair, Integer> updateHistory; // History of updates

    // Constructor to initialize replicaID, coordinator, and update history
    public Replica(String replicaID, ActorRef coordinator) {
        this.replicaID = replicaID;
        this.coordinator = coordinator;
        this.updateHistory = new HashMap<>();
    }

    // Factory method to create a Replica actor
    public static Props props(String replicaID, ActorRef coordinator) {
        return Props.create(Replica.class, () -> new Replica(replicaID, coordinator));
    }

    // Message classes for communication between actors
    public static class ReadRequest implements Serializable {
        public final ActorRef client;

        public ReadRequest(ActorRef client) {
            this.client = client;
        }
    }

    public static class WriteRequest implements Serializable {
        public final ActorRef client;
        public final int newValue;

        public WriteRequest(ActorRef client, int newValue) {
            this.client = client;
            this.newValue = newValue;
        }
    }

    public static class Update implements Serializable {
        public final EpochSequencePair epochSequencePair;
        public final int newValue;

        public Update(EpochSequencePair epochSequencePair, int newValue) {
            this.epochSequencePair = epochSequencePair;
            this.newValue = newValue;
        }
    }

    public static class Ack implements Serializable {
        public final EpochSequencePair epochSequencePair;

        public Ack(EpochSequencePair epochSequencePair) {
            this.epochSequencePair = epochSequencePair;
        }
    }

    // Method to handle incoming messages
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadRequest.class, this::onReadRequest)
                .match(WriteRequest.class, this::onWriteRequest)
                .match(Update.class, this::onUpdate)
                .match(Ack.class, this::onAck)
                .build();
    }

    // Handle read requests from clients
    private void onReadRequest(ReadRequest request) {
        System.out.println("Replica " + replicaID + " received read request from Client.");
        request.client.tell(new Coordinator.ReadResponse(value), getSelf());
    }

    // Handle write requests from clients and forward them to the coordinator
    private void onWriteRequest(WriteRequest request) {
        System.out.println("Replica " + replicaID + " received write request from Client with value: " + request.newValue);
        coordinator.tell(new Coordinator.WriteRequest(getSelf(), request.newValue), getSelf());
    }

    // Handle update messages from the coordinator
    private void onUpdate(Update update) {
        System.out.println("Replica " + replicaID + " received update: " + update.epochSequencePair + " with value: " + update.newValue);
        updateHistory.put(update.epochSequencePair, update.newValue);
        value = update.newValue; // Apply the update
        coordinator.tell(new Ack(update.epochSequencePair), getSelf()); // Send an acknowledgment back to the coordinator
    }

    // Handle ACK messages from the coordinator
    private void onAck(Ack ack) {
        System.out.println("Replica " + replicaID + " received ACK for: " + ack.epochSequencePair);
    }
}
