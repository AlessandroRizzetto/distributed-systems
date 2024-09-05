package it.ds1;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Coordinator extends AbstractActor {
    private int epoch = 0;
    private int seqNumber = 0;
    private final int quorumSize;
    private final Map<Integer, ActorRef> replicas = new HashMap<>();
    private Set<ActorRef> acks = new HashSet<>();

    public Coordinator(int N) {
        this.quorumSize = N / 2 + 1;
        Main.customPrint("Quorum size: " + quorumSize);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Client.WriteRequest.class, this::onWriteRequest)
                .match(Replica.Ack.class, this::onAck)
                .match(Replica.Register.class, this::onRegisterReplica)
                .build();
    }

    private void onRegisterReplica(Replica.Register msg) {
        replicas.put(msg.id, getSender());
        Main.customPrint("Coordinator registered Replica " + msg.id);
    }

    private void onWriteRequest(Client.WriteRequest msg) {
        EpochSequencePair updateId = new EpochSequencePair(epoch, seqNumber++);
        Main.customPrint("Coordinator received write request for replica " + replicas.entrySet().stream()
            .filter(entry -> entry.getValue().equals(msg.targetReplica)).findFirst().orElse(null) + " with value: " + msg.newValue);
        acks.clear();
    
        // Broadcast update to all replicas
        replicas.values().forEach(replica -> replica.tell(new Replica.Update(updateId, msg.newValue), getSelf()));
    }

    private void onAck(Replica.Ack msg) {
        acks.add(msg.replica);
        Main.customPrint("Coordinator received ACK from Replica " + replicas.entrySet().stream()
            .filter(entry -> entry.getValue().equals(msg.replica)).findFirst().orElse(null));
        if (acks.size() >= quorumSize) {
            if (!acks.isEmpty()) { // Assicurati che l'invio avvenga una sola volta
                Main.customPrint("Quorum reached, broadcasting WRITEOK.");
                replicas.values().forEach(replica -> replica.tell(new Replica.WriteOk(), getSelf()));
                acks.clear(); // Svuota la lista di acks per evitare ripetizioni
            }
        }
    }
}
