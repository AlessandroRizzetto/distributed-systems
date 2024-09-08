package it.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;

import akka.actor.Cancellable;

public class Coordinator extends AbstractActor {
    private int epoch = 0;
    private int seqNumber = 0;
    private final int quorumSize;
    private final Map<Integer, ActorRef> replicas = new HashMap<>();
    private Set<ActorRef> acks = new HashSet<>();
    private Cancellable heartbeatSchedule;
    private int isCrashed = 0;
    public static boolean testAckCrash = false;

    public Coordinator(int N, EpochSequencePair epochSequencePair) {
        this.quorumSize = N / 2 + 1;
        this.epoch = epochSequencePair.epoch;
        this.seqNumber = epochSequencePair.seqNum;
        Main.customPrint("Quorum size: " + quorumSize);
        startHeartbeatSchedule();
    }

    private void startHeartbeatSchedule() {

        heartbeatSchedule = getContext().system().scheduler().scheduleAtFixedRate(
                Duration.Zero(),
                Duration.create(4, TimeUnit.SECONDS),
                this::sendHeartbeat,
                getContext().dispatcher());
    }

    private void sendHeartbeat() {
        if (this.isCrashed == 0) {
            replicas.values().forEach(replica -> replica.tell(new Replica.Heartbeat(), getSelf()));
            Main.customPrint("Heartbeat sent by Coordinator");
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Client.WriteRequest.class, this::onWriteRequest)
                .match(Replica.Ack.class, this::onAck)
                .match(Replica.Register.class, this::onRegisterReplica)
                .match(Crash.class, this::onCrash)
                .match(Replica.UpdateEpoch.class, this::onUpdateEpoch)
                .build();
    }

    private void onRegisterReplica(Replica.Register msg) {
        Main.customPrint("New replicas size: " + replicas.size());
        Main.customPrint("New replicas: " + replicas);
        replicas.put(msg.id, getSender());
        Main.customPrint("Coordinator registered Replica " + msg.id);
    }

    private void onWriteRequest(Client.WriteRequest msg) {
        EpochSequencePair updateId = new EpochSequencePair(epoch, seqNumber++);
        Main.customPrint("Coordinator received write request for replica " + replicas.entrySet().stream()
                .filter(entry -> entry.getValue().equals(msg.targetReplica)).findFirst().orElse(null) + " with value: "
                + msg.newValue);
        acks.clear();

        // Simulate a Coordinator crash after receiving the write request from a replica
        onCrash(new Crash(0));
        if (this.isCrashed == 0) {
            Main.customPrint("the Coordinator is sending update messages");
            replicas.values().forEach(replica -> replica.tell(new Replica.Update(updateId, msg.newValue), getSelf()));
        }
    }

    private void onAck(Replica.Ack msg) {
        acks.add(msg.replica);
        Main.customPrint("Coordinator received ACK from Replica " + replicas.entrySet().stream()
                .filter(entry -> entry.getValue().equals(msg.replica)).findFirst().orElse(null));
        if (acks.size() >= quorumSize) {
            if (!acks.isEmpty()) {
                // Simulate a Coordinator crash by calling the onCrash method
                if (testAckCrash) {
                    Main.customPrint("Coordinator during ACK CRASH simulated!!!");
                    onCrash(new Crash(0));
                    testAckCrash = false;
                }
                if (this.isCrashed == 0) {
                    Main.customPrint("Quorum reached, broadcasting WRITEOK.");
                    replicas.values().forEach(replica -> replica.tell(new Replica.WriteOk(), getSelf()));
                    acks.clear();
                }
            }
        }
    }

    private void onCrash(Crash crash) {
        Main.customPrint("Coordinator CRASH simulated!!!");
        this.isCrashed = 1;
        getContext().become(crashed());
    }

    private void onUpdateEpoch(Replica.UpdateEpoch msg) {
        if (msg.epoch > epoch) {
            Main.customPrint("Updating coordinator epoch from " + epoch + " to " + msg.epoch);
            epoch = msg.epoch;
            seqNumber = 0;
        }
    }

    public Receive crashed() {
        return receiveBuilder()
                .matchAny(msg -> {
                })
                .build();
    }

    public static class Heartbeat {
    }

    public static class Crash {
        final int duration;

        public Crash(int duration) {
            this.duration = duration;
        }
    }
}
