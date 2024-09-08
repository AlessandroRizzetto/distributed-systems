package it.ds1;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import java.util.List;

public class Client extends AbstractActor {
    private final List<ActorRef> replicas;
    private final ActorRef coordinator;

    public Client(List<ActorRef> replicas, ActorRef coordinator) {
        this.replicas = replicas;
        this.coordinator = coordinator;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadRequest.class, this::onReadRequest)
                .match(ReadDone.class, this::onReadDone)
                .match(WriteRequest.class, this::onWriteRequest)
                .build();
    }

    private void onReadRequest(ReadRequest msg) {
        Main.customPrint("Client read request to Replica " + replicas.indexOf(msg.replica));
        msg.replica.tell(new Client.ReadRequest(msg.replica), getSelf());
    }

    private void onReadDone(ReadDone msg) {
        Main.customPrint("Client read done, value: " + msg.value);
    }

    private void onWriteRequest(WriteRequest msg) {
        Main.customPrint("Client write request to Replica " + replicas.indexOf(msg.targetReplica) + " with new value: " + msg.newValue);
        msg.targetReplica.tell(new Replica.Write(msg.newValue), getSelf());  // Send the write request to the target replica
    }
    

    public static class ReadRequest {
        final ActorRef replica;

        public ReadRequest(ActorRef replica) {
            this.replica = replica;
        }
    }

    public static class ReadDone {
        final int value;

        public ReadDone(int value) {
            this.value = value;
        }
    }

    public static class WriteRequest {
        final ActorRef targetReplica;
        final int newValue;
    
        public WriteRequest(ActorRef targetReplica, int newValue) {
            this.targetReplica = targetReplica;
            this.newValue = newValue;
        }
    }
}
