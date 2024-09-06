package it.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ReceiveTimeout;
import akka.io.Tcp.Write;
import java.time.Duration;

import java.util.HashMap;
import java.util.Map;

public class Replica extends AbstractActor {
    private int id;
    private int localValue;
    private EpochSequencePair lastUpdate;
    private ActorRef coordinator;
    private Map<EpochSequencePair, Integer> history = new HashMap<>();
    private final long TIMEOUT_DURATION = 10;

    public Replica(int id, ActorRef coordinator) {
        this.id = id;
        this.coordinator = coordinator;
        this.coordinator.tell(new Register(id), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Client.ReadRequest.class, this::onReadRequest)
                .match(Update.class, this::onUpdate)
                .match(WriteOk.class, this::onWriteOk)
                .match(Write.class, this::onWrite)
                .match(ReceiveTimeout.class, r -> {
                    // il coordinatore e' crashato!
                    // da qui bisognerebbe partire con l'algoritmo di election
                    this.cancelTimeout();
                })
                .build();
    }

    private void onReadRequest(Client.ReadRequest msg) {
        Main.customPrint("Client read request to Replica " + id);
        getSender().tell(new Client.ReadDone(localValue), getSelf());
    }

    private void onUpdate(Update msg) {
        this.cancelTimeout(); // ho ricevuto UPDATE dal coordinatore, quindi posso continuare
        if (!history.containsKey(msg.updateId)) {
            Main.customPrint("Replica " + id + " received update: " + msg.updateId + " with value " + msg.newValue);
            lastUpdate = msg.updateId;
            history.put(msg.updateId, msg.newValue);
            getSender().tell(new Ack(getSelf()), getSelf());
            getContext().setReceiveTimeout(Duration.ofSeconds(this.TIMEOUT_DURATION)); // attendo WRITEOK
        }
    }

    private void onWriteOk(WriteOk msg) {
        this.cancelTimeout(); // ho ricevuto WRITEOK
        Main.customPrint("Replica " + id + " applying update.");
        if (history.containsKey(lastUpdate) && localValue != history.get(lastUpdate)) {
            localValue = history.get(lastUpdate);
            Main.customPrint("Replica " + id + " updated value to " + localValue);
        }
    }

    private void onWrite(Write msg) {
        Main.customPrint("Replica " + id + " received write request with value " + msg.newValue);
        coordinator.tell(new Client.WriteRequest(getSelf(), msg.newValue), getSelf()); // Inoltra al coordinatore
        getContext().setReceiveTimeout(Duration.ofSeconds(this.TIMEOUT_DURATION)); // attendo UPDATE
    }

    private void cancelTimeout() {
        getContext().cancelReceiveTimeout(); // annulla il timeout
    }

    public static class Write {
        final int newValue;

        public Write(int newValue) {
            this.newValue = newValue;
        }
    }

    public static class Update {
        final EpochSequencePair updateId;
        final int newValue;

        public Update(EpochSequencePair updateId, int newValue) {
            this.updateId = updateId;
            this.newValue = newValue;
        }
    }

    public static class Ack {
        final ActorRef replica;

        public Ack(ActorRef replica) {
            this.replica = replica;
        }
    }

    public static class WriteOk {
    }

    public static class Register {
        final int id;

        public Register(int id) {
            this.id = id;
        }
    }

    public static class Heartbeat {
    }
}
