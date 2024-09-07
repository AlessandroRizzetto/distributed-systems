package it.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import scala.collection.immutable.List;
import scala.collection.mutable.LinkedHashSet.Entry;
import scala.concurrent.duration.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.*;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.ds1.Coordinator.Crash;

public class Replica extends AbstractActor {
    private int id;
    private int localValue;
    private EpochSequencePair lastUpdate;
    private ActorRef coordinator;
    private Map<EpochSequencePair, Integer> history = new HashMap<>();
    private HashMap<Integer, ActorRef> replicasMap = new HashMap<>();
    private Set<Integer> crashedReplicas = new HashSet<>();

    private Cancellable heartbeatTimer;
    private Cancellable writeOkTimeout;
    private Cancellable writeRequestTimeout;
    private Cancellable electionTimeout;
    private boolean isCrashed = false;

    public Replica(int id, ActorRef coordinator) {
        this.id = id;
        this.coordinator = coordinator;
        this.coordinator.tell(new Register(id), getSelf()); // Registra la replica presso il coordinatore
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Client.ReadRequest.class, this::onReadRequest)
                .match(Update.class, this::onUpdate)
                .match(WriteOk.class, this::onWriteOk)
                .match(Write.class, this::onWrite)
                .match(Heartbeat.class, this::onHeartbeat)
                .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
                .match(WriteOkTimeout.class, this::onWriteOkTimeout)
                .match(WriteRequestTimeout.class, this::onWriteRequestTimeout)
                .match(Register.class, this::onRegister) // Aggiungi il gestore per il messaggio di registrazione
                .match(AddReplicaRequest.class, this::onAddReplica)
                .match(Crash.class, this::onCrash)
                .match(Election.class, this::onElection)
                .build();
    }

    // Metodo centralizzato per pianificare i timeout
    private Cancellable scheduleTimeout(Cancellable currentTimeout, Object timeoutMessage, int delayInSeconds) {
        // Main.customPrint("Scheduling timeout for " +
        // timeoutMessage.getClass().getSimpleName() + " at Replica " + id);
        if (currentTimeout != null && !currentTimeout.isCancelled()) {
            currentTimeout.cancel(); // Cancella il timeout precedente se attivo
        }
        return getContext().system().scheduler().scheduleOnce(
                Duration.create(delayInSeconds, TimeUnit.SECONDS),
                getSelf(),
                timeoutMessage,
                getContext().dispatcher(),
                getSelf());
    }

    private void onRegister(Register msg) {
        resetHeartbeatTimer(); // Avvia il timer del heartbeat solo dopo la registrazione
    }

    // Pianifica il timeout per il messaggio WRITEOK
    private void startWriteOkTimeout() {
        writeOkTimeout = scheduleTimeout(writeOkTimeout, new WriteOkTimeout(), 5); // Timeout di 5 secondi per aspettare
                                                                                   // il WRITEOK
    }

    // Pianifica il timeout per l'inoltro della richiesta di scrittura
    private void startWriteRequestTimeout() {
        writeRequestTimeout = scheduleTimeout(writeRequestTimeout, new WriteRequestTimeout(), 5); // Timeout di 5
                                                                                                  // secondi per il
                                                                                                  // broadcast
    }

    private void startElectionTimeout(int replicaId) {
        writeRequestTimeout = scheduleTimeout(electionTimeout, new ElectionTimeout(replicaId), 2); // Timeout di 2
        // secondi per il
        // broadcast
    }

    // Gestisce il messaggio Write
    private void onWrite(Write msg) {
        Main.customPrint("Replica " + id + " received write request with value " + msg.newValue);
        coordinator.tell(new Client.WriteRequest(getSelf(), msg.newValue), getSelf()); // Inoltra la richiesta al
                                                                                       // coordinatore
        startWriteRequestTimeout(); // Avvia il timeout per aspettare il broadcast dal coordinatore
    }

    // Gestisce il messaggio WriteOk
    private void onWriteOk(WriteOk msg) {
        Main.customPrint("Replica " + id + " received WRITEOK.");
        cancelWriteOkTimeout(); // Cancella il timeout dopo la ricezione del WRITEOK

        if (history.containsKey(lastUpdate) && localValue != history.get(lastUpdate)) {
            localValue = history.get(lastUpdate);
            Main.customPrint("Replica " + id + " updated value to " + localValue);
        }
    }

    // Gestisce la richiesta di lettura del client
    private void onReadRequest(Client.ReadRequest msg) {
        Main.customPrint("Client read request to Replica " + id);
        getSender().tell(new Client.ReadDone(localValue), getSelf());
    }

    // Gestisce l'aggiornamento da parte del coordinatore
    private void onUpdate(Update msg) {
        if (!history.containsKey(msg.updateId)) {
            Main.customPrint("Replica " + id + " received update: " + msg.updateId + " with value " + msg.newValue);
            lastUpdate = msg.updateId;
            history.put(msg.updateId, msg.newValue);
            getSender().tell(new Ack(getSelf()), getSelf());
            cancelWriteRequestTimeout(); // Delete the write request timeout
            startWriteOkTimeout(); // Avvia il timeout per aspettare il WRITEOK
        }
    }

    // Gestistione del messaggio Heartbeat
    private void onHeartbeat(Heartbeat heartbeat) {
        Main.customPrint("Heartbeat received at Replica " + id);
        resetHeartbeatTimer(); // Reimposta il timer del heartbeat quando viene ricevuto un heartbeat
    }

    // Metodo per reimpostare il timer del heartbeat
    private void resetHeartbeatTimer() {
        heartbeatTimer = scheduleTimeout(heartbeatTimer, new HeartbeatTimeout(), 100); // Timeout di 10 secondi per il
                                                                                       // messaggio heartbeat
    }

    // Gestione della cancellazione dei timeout
    private void cancelWriteOkTimeout() {
        if (writeOkTimeout != null) {
            writeOkTimeout.cancel(); // Cancella il timeout attivo per il WRITEOK
            writeOkTimeout = null;
        }
    }

    private void cancelWriteRequestTimeout() {
        if (writeRequestTimeout != null) {
            writeRequestTimeout.cancel(); // Cancella il timeout attivo per la richiesta di scrittura
            writeRequestTimeout = null;
        }
    }

    private void cancelElectionTimeout() {
        if (electionTimeout != null) {
            electionTimeout.cancel(); // Cancella il timeout attivo per la richiesta di scrittura
            electionTimeout = null;
        }
    }

    // Gestione del timeout quando il coordinatore non invia il WRITEOK
    private void onWriteOkTimeout(WriteOkTimeout msg) {
        if (isCrashed) {
            return;
        }
        Main.customPrint("Timeout while waiting for WRITEOK at Replica " + id + ": Coordinator crash suspected");
        // Gestione del crash o avvio di un'elezione
    }

    // Gestione del timeout quando il coordinatore non avvia il broadcast
    private void onWriteRequestTimeout(WriteRequestTimeout msg) {
        if (isCrashed) {
            return;
        }
        Main.customPrint("Timeout while waiting for coordinator to broadcast WRITE request at Replica " + id
                + ": Coordinator crash suspected");
        // Gestione del crash o avvio di un'elezione
        startElection();
    }

    private void onElectionTimeout(ElectionTimeout electionTimeout) {
        if (isCrashed) {
            return;
        }
        crashedReplicas.add(electionTimeout.replicaId);
        replicasMap.remove(electionTimeout.replicaId);
        startElection();
    }

    // Gestione del timeout generale
    private void onHeartbeatTimeout(HeartbeatTimeout timeout) {
        if (isCrashed) {
            return;
        }
        Main.customPrint("Timeout while waiting for coordinator to broadcast HEARTBEAT request at Replica " + id
                + ": Coordinator crash suspected");
        // Potenziale codice per avviare l'elezione o altre azioni
    }

    private void onCrash(Crash crash) {
        if (!isCrashed) {
            Main.customPrint("Replica " + id + " CRASH simulated!!!");
        }
        this.isCrashed = true;
        getContext().become(crashed());
    }

    private void onElection(Election election) {
        getContext().become(election());
        Main.customPrint("Replica " + id + " received election message");
        var currentState = election.state;

        if (currentState.containsKey(id)) {
            Main.customPrint("Replica " + id + " ring completed");
            ActorRef nextCoordinator = null;
            Map.Entry<Integer, EpochSequencePair> mostRecentUpdate = null;
            for (var entry : currentState.entrySet()) {
                if (mostRecentUpdate == null ||
                        (entry.getValue().seqNum > mostRecentUpdate.getValue().seqNum)) {
                    mostRecentUpdate = entry;
                    Main.customPrint(
                            "Replica " + id + " with lastest update " + mostRecentUpdate.getValue().seqNum + "");
                } else if (entry.getValue().seqNum == mostRecentUpdate.getValue().seqNum) {
                    if (entry.getKey() > mostRecentUpdate.getKey()) {
                        mostRecentUpdate = entry;
                        Main.customPrint(
                                "Replica " + id + " has the same sequence id, but the most recent is "
                                        + mostRecentUpdate.getKey());
                    }
                }
            }

            nextCoordinator = replicasMap.get(mostRecentUpdate.getKey());

            Main.customPrint(
                    "Replica " + id + " is " + getSelf());
            Main.customPrint(
                    "Next coordinator is " + nextCoordinator);

            if (nextCoordinator == null) {
                nextCoordinator = getSelf();
                // create the new coordinator with all the necesssary data
                Main.customPrint("Replica " + id + " is the new coordinator");
                var newCoordinator = Main.system.actorOf(
                        Props.create(Coordinator.class, replicasMap.size() - 1,
                                new EpochSequencePair(mostRecentUpdate.getValue().epoch + 1, 0)),
                        "coordinator" + (mostRecentUpdate.getValue().epoch + 1));
                for (var replica : replicasMap.entrySet()) {
                    Main.customPrint("Coordinator is adding " + replica.getKey());
                    newCoordinator.tell(new Register(replica.getKey()), replica.getValue()); // Registra la replica
                                                                                             // presso il
                    // coordinatore
                    replica.getValue().tell(new RegisterNewCoordinator(newCoordinator), getSelf());
                    Main.customPrint("Replica " + id + " sent sync message to the replica " + replica.getKey());
                    replica.getValue().tell(new Replica.Sync(crashedReplicas), getSelf());
                    onCrash(new Crash(0));
                    isCrashed = true;
                }
            } else {
                // just have to forward the message to the next replica
                Main.customPrint("Replica " + id + " is forwarding the election message");
                var nextReplica = getNextReplica();
                nextReplica.getValue().tell(new Election(new HashMap<Integer, EpochSequencePair>(currentState)),
                        getSelf());
                startElectionTimeout(nextReplica.getKey());
            }
        } else {
            // add the information of the replica and forward the message to the next
            // replica
            Main.customPrint("Replica " + id + " is adding its last update to the election message");
            currentState.put(id, lastUpdate);
            var nextReplica = getNextReplica();
            nextReplica.getValue().tell(new Election(new HashMap<Integer, EpochSequencePair>(currentState)), getSelf());
            startElectionTimeout(nextReplica.getKey());
        }

        getSender().tell(new Replica.Ack(getSelf()), getSelf());
    }

    private void onRegisterNewCoordinator(Replica.RegisterNewCoordinator registerNewCoordinator) {
        this.coordinator = registerNewCoordinator.newCoordinator;
    }

    private void onSync(Replica.Sync sync) {
        Main.customPrint("Replica " + id + " received the sync message");
        for (var id : sync.crashedReplicas) {
            replicasMap.remove(id);
        }
        getContext().become(createReceive());
    }

    public Receive crashed() {
        return receiveBuilder()
                .matchAny(msg -> {
                })
                .build();
    }

    private void startElection() {
        getContext().become(election());
        Main.customPrint("Replica " + id + " is in election state");

        var nextReplica = getNextReplica();
        nextReplica.getValue().tell(new Election(new HashMap<Integer, EpochSequencePair>() {
            {
                put(id, lastUpdate);
            }
        }), getSelf());

        startElectionTimeout(nextReplica.getKey());
    }

    private Map.Entry<Integer, ActorRef> getNextReplica() {
        var replicasIds = new ArrayList<Integer>(replicasMap.keySet());
        int nextReplicaId = findNextGreater(replicasIds, id);
        ActorRef nextReplica = replicasMap.get(nextReplicaId);

        return new java.util.AbstractMap.SimpleEntry<Integer, ActorRef>(nextReplicaId, nextReplica);
    }

    private Integer findNextGreater(ArrayList<Integer> replicasIds, int id) {
        Integer nextGreater = null;

        // Iterate through each number in the sequence
        var min = Collections.min(replicasIds);
        for (int num : replicasIds) {
            // Find the smallest number greater than the given value
            if (num > id && (nextGreater == null || num < nextGreater)) {
                nextGreater = num;
            }
        }

        if (nextGreater == null) {
            nextGreater = min;
        }

        return nextGreater;
    }

    public Receive election() {
        return receiveBuilder()
                // accettare gli ACK, SYNCRONIZATION e ELECTION
                .match(Replica.Ack.class, this::onAck)
                .match(Replica.ElectionTimeout.class, this::onElectionTimeout)
                .match(Replica.RegisterNewCoordinator.class, this::onRegisterNewCoordinator)
                .match(Election.class, this::onElection)
                .match(Replica.Sync.class, this::onSync)
                .build();
    }

    private void onAck(Replica.Ack msg) {
        cancelElectionTimeout();
    }

    public void onAddReplica(AddReplicaRequest addReplica) {
        addReplica.replicasMap.remove(this.id);
        this.replicasMap = addReplica.replicasMap;
        Main.customPrint("Replica with id " + id + " is adding replicas:");
        for (Map.Entry<Integer, ActorRef> entry : replicasMap.entrySet()) {
            Main.customPrint("Replica with id " + entry.getKey() + " added for " + id);
        }
    }

    // Definizione dei messaggi utilizzati per i timeout
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

    public static class Sync {
        final Set<Integer> crashedReplicas;

        public Sync(Set<Integer> crashedReplicas) {
            this.crashedReplicas = crashedReplicas;
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

    public static class RegisterNewCoordinator {
        final ActorRef newCoordinator;

        public RegisterNewCoordinator(ActorRef newCoordinator) {
            this.newCoordinator = newCoordinator;
        }
    }

    public static class AddReplicaRequest {
        final HashMap<Integer, ActorRef> replicasMap;

        public AddReplicaRequest(HashMap<Integer, ActorRef> replicasMap) {
            this.replicasMap = replicasMap;
        }
    }

    public static class Timeout {
    }

    public static class Heartbeat {
    }

    public static class WriteOkTimeout {
    }

    public static class WriteRequestTimeout {
    }

    public static class ElectionTimeout {
        final int replicaId;

        public ElectionTimeout(int replicaId) {
            this.replicaId = replicaId;
        }

    }

    public static class HeartbeatTimeout {
    }

    public static class Crash {
        final int duration;

        public Crash(int duration) {
            this.duration = duration;
        }
    }

    public static class Election {
        final HashMap<Integer, EpochSequencePair> state;

        public Election(HashMap<Integer, EpochSequencePair> state) {
            this.state = state;
        }
    }
}
