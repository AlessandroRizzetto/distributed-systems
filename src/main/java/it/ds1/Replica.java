package it.ds1;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.io.Tcp.Write;

import java.util.HashMap;
import java.util.Map;

public class Replica extends AbstractActor {
    private int id;
    private int localValue;
    private EpochSequencePair lastUpdate;
    private ActorRef coordinator;
    private Map<EpochSequencePair, Integer> history = new HashMap<>();

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
                .build();
    }

    private void onReadRequest(Client.ReadRequest msg) {
        Main.customPrint("Client read request to Replica " + id);
        getSender().tell(new Client.ReadDone(localValue), getSelf());
    }

    private void onUpdate(Update msg) {
        if (!history.containsKey(msg.updateId)) {
            Main.customPrint("Replica " + id + " received update: " + msg.updateId + " with value " + msg.newValue);
            lastUpdate = msg.updateId;
            history.put(msg.updateId, msg.newValue);
            getSender().tell(new Ack(getSelf()), getSelf());
        }
    }
    

    private void onWriteOk(WriteOk msg) {
        Main.customPrint("Replica " + id + " applying update.");
        if (history.containsKey(lastUpdate) && localValue != history.get(lastUpdate)) {
            localValue = history.get(lastUpdate);
            Main.customPrint("Replica " + id + " updated value to " + localValue);
        }
    }
    

    private void onWrite(Write msg) {
        Main.customPrint("Replica " + id + " received write request with value " + msg.newValue);
        coordinator.tell(new Client.WriteRequest(getSelf(), msg.newValue), getSelf());  // Inoltra al coordinatore
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

    public static class WriteOk { }

    public static class Register {
        final int id;

        public Register(int id) {
            this.id = id;
        }
    }
}
