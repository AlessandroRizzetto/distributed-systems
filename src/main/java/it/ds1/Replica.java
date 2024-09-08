package it.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;
import java.util.*;
import akka.actor.Cancellable;
import akka.actor.Props;

public class Replica extends AbstractActor {
    private int id;
    private int localValue;
    private EpochSequencePair lastUpdate = new EpochSequencePair(0, 0);
    private ActorRef coordinator;
    private Map<EpochSequencePair, Integer> history = new HashMap<>();
    private HashMap<Integer, ActorRef> replicasMap = new HashMap<>();
    private Set<Integer> crashedReplicas = new HashSet<>();

    private Cancellable heartbeatTimer;
    private Cancellable writeOkTimeout;
    private Cancellable writeRequestTimeout;
    private Cancellable electionTimeout;
    private Cancellable ringElectionTimeout;
    private boolean isCrashed = false;
    private boolean isNewCoordinatorElected = false;
    private boolean underElection = false;
    private boolean updateToComplete = false;
    private boolean isForwardingMessage = false;
    public static boolean isNextReplicaCrashedTest = false;

    public Replica(int id, ActorRef coordinator) {
        this.id = id;
        this.coordinator = coordinator;
        this.coordinator.tell(new Register(id), getSelf()); // Register the replica on the coordinator
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
                .match(Register.class, this::onRegister)
                .match(AddReplicaRequest.class, this::onAddReplica)
                .match(Crash.class, this::onCrash)
                .match(Election.class, this::onElection)
                .match(Replica.ElectionTimeout.class, this::onElectionTimeout2)
                .match(Replica.Ack.class, this::onAck)
                .match(Replica.RingElectionTimeout.class, this::onRingElectionTimeout)
                .build();
    }

    public Receive crashed() {
        return receiveBuilder()
                .matchAny(msg -> {
                })
                .build();
    }

    public Receive election() {
        return receiveBuilder()
                .match(Replica.Ack.class, this::onAck)
                .match(Replica.ElectionTimeout.class, this::onElectionTimeout)
                .match(Replica.RegisterNewCoordinator.class, this::onRegisterNewCoordinator)
                .match(Election.class, this::onElection)
                .match(Replica.Sync.class, this::onSync)
                .match(Replica.Update.class, this::onUpdate)
                .match(Replica.RingElectionTimeout.class, this::onRingElectionTimeout)
                .match(Heartbeat.class, this::onHeartbeat)
                .match(Crash.class, this::onCrash)
                .build();
    }

    // Main method to schedule the timeouts
    private Cancellable scheduleTimeout(Cancellable currentTimeout, Object timeoutMessage, int delayInSeconds) {
        if (currentTimeout != null && !currentTimeout.isCancelled()) {
            currentTimeout.cancel();
        }
        return getContext().system().scheduler().scheduleOnce(
                Duration.create(delayInSeconds, TimeUnit.SECONDS),
                getSelf(),
                timeoutMessage,
                getContext().dispatcher(),
                getSelf());
    }

    private void onRegister(Register msg) {
        resetHeartbeatTimer();
    }

    // Schedule the timeout for the WRITEOK message
    private void startWriteOkTimeout() {
        writeOkTimeout = scheduleTimeout(writeOkTimeout, new WriteOkTimeout(), 5);
    }

    // Schedule the timeout for the WriteRequest message
    private void startWriteRequestTimeout() {
        writeRequestTimeout = scheduleTimeout(writeRequestTimeout, new WriteRequestTimeout(), 5);
    }

    // Schedule the timeout for the Election message
    private void startElectionTimeout(int replicaId) {
        writeRequestTimeout = scheduleTimeout(electionTimeout, new ElectionTimeout(replicaId), 2);
    }

    // Schedule the timeout for the RingElection message
    private void startRingElectionTimeout() {
        ringElectionTimeout = scheduleTimeout(ringElectionTimeout, new RingElectionTimeout(), 20);
    }

    // Reset the timeout for the heartbeat message
    private void resetHeartbeatTimer() {
        heartbeatTimer = scheduleTimeout(heartbeatTimer, new HeartbeatTimeout(), 20);
    }

    // Manage the Write message
    private void onWrite(Write msg) {

        Main.customPrint("Replica " + id + " received write request with value " + msg.newValue);
        coordinator.tell(new Client.WriteRequest(getSelf(), msg.newValue), getSelf());
        startWriteRequestTimeout(); // Start timeout while waiting for the broadcast of the coordinator
    }

    // Manage the WRITEOK message
    private void onWriteOk(WriteOk msg) {
        Main.customPrint("Replica " + id + " received WRITEOK.");
        cancelWriteOkTimeout(); // Cancel the timout after receiving the WRITEOK message

        if (history.containsKey(lastUpdate) && localValue != history.get(lastUpdate)) {
            localValue = history.get(lastUpdate);
            Main.customPrint("Replica " + id + " updated value to " + localValue);
        }
    }

    // Manage the client read request
    private void onReadRequest(Client.ReadRequest msg) {
        Main.customPrint("Client read request to Replica " + id);
        getSender().tell(new Client.ReadDone(localValue), getSelf());
    }

    // Manage the update for the coordinator
    private void onUpdate(Update msg) {
        Main.customPrint("Replica " + id + " received update: " + msg.updateId + " with value " + msg.newValue);
        if (history.containsKey(msg.updateId)) {
            Main.customPrint("Replica " + id + " already has the update: " + msg.updateId);
            coordinator.tell(new Ack(getSelf()), getSelf());
        } else {
            history.put(msg.updateId, msg.newValue);
            lastUpdate = msg.updateId;
            getSender().tell(new Ack(getSelf()), getSelf());
        }

        cancelWriteRequestTimeout();
        startWriteOkTimeout();
    }

    // Manage the Heartbeat message
    private void onHeartbeat(Heartbeat heartbeat) {
        Main.customPrint("Heartbeat received at Replica " + id);
        resetHeartbeatTimer();
    }

    // Manage the cancellation of the WRITEOK message
    private void cancelWriteOkTimeout() {
        if (writeOkTimeout != null) {
            writeOkTimeout.cancel();
            writeOkTimeout = null;
        }
    }

    // Manage the cancellation of the WriteRequest message
    private void cancelWriteRequestTimeout() {
        if (writeRequestTimeout != null) {
            writeRequestTimeout.cancel();
            writeRequestTimeout = null;
        }
    }

    // Manage the cancellation of the Election message
    private void cancelElectionTimeout() {
        if (electionTimeout != null) {
            electionTimeout.cancel();
            electionTimeout = null;
        }
    }

    // Manage the cancellation of the RingElection message
    private void cancelRingElectionTimeout() {
        if (ringElectionTimeout != null) {
            ringElectionTimeout.cancel();
            ringElectionTimeout = null;
        }
    }

    // Manage the timeout when the coordinator does not send the WRITEOK message
    private void onWriteOkTimeout(WriteOkTimeout msg) {
        if (isCrashed) {
            return;
        }

        Main.customPrint("Timeout while waiting for WRITEOK at Replica " + id + ": Coordinator crash suspected");
        isNewCoordinatorElected = false;
        underElection = true;
        updateToComplete = true;
        startElection(); // In this case we need to finish the update request of the old coordinator
                         // before starting the new epoch
    }

    // Manage the timeout when the coordinator does not send the broadcast
    private void onWriteRequestTimeout(WriteRequestTimeout msg) {
        if (isCrashed) {
            return;
        }
        Main.customPrint("Timeout while waiting for coordinator to broadcast WRITE request at Replica " + id
                + ": Coordinator crash suspected");
        isNewCoordinatorElected = false;
        underElection = true;
        startElection();
    }

    // Manage the timeout when a replica does not send the ACK during election
    private void onElectionTimeout(ElectionTimeout electionTimeout) {
        if (isCrashed) {
            return;
        }
        crashedReplicas.add(electionTimeout.replicaId);
        replicasMap.remove(electionTimeout.replicaId);
        isNewCoordinatorElected = false;
        underElection = true;
        startElection();
    }

    // Manage the timeout for the entire election process
    private void onRingElectionTimeout(RingElectionTimeout timeout) {
        if (isCrashed) {
            return;
        }
        Main.customPrint(
                "Timeout while waiting for the ring to complete at Replica " + id + ": Coordinator crash suspected");
        isNewCoordinatorElected = false;
        underElection = true;
        startElection();
    }

    // Manage the timeout when the replica is not in election state
    private void onElectionTimeout2(ElectionTimeout electionTimeout) {
    }

    // Manage the timeout for the heartbeat
    private void onHeartbeatTimeout(HeartbeatTimeout timeout) {
        if (isCrashed) {
            return;
        }
        Main.customPrint("Timeout while waiting for coordinator to broadcast HEARTBEAT request at Replica " + id
                + ": Coordinator crash suspected");
        isNewCoordinatorElected = false;
        underElection = true;
        startElection();
    }

    // Handler for the passage into the crash state
    private void onCrash(Crash crash) {
        if (!isCrashed) {
            Main.customPrint("Replica " + id + " CRASH simulated!!!");
        }
        this.isCrashed = true;
        getContext().become(crashed());
    }

    // Handler for the passage into the crash state at the moment of the election of
    // the new coordinator
    private void onCrashForElection(Crash crash) {
        if (!isCrashed) {
            Main.customPrint("Replica " + id + " CRASH simulated for coordinator election!!!");
        }
        this.isCrashed = true;
        getContext().become(crashed());
    }

    // Handler that implements the ring algorithm
    private void onElection(Election election) {
        if (isNewCoordinatorElected) {
            return;
        }
        if (isNextReplicaCrashedTest) {
            isNextReplicaCrashedTest = false;
            getSelf().tell(new Replica.Crash(0), ActorRef.noSender());
            return;
        }
        getContext().become(election());
        Main.customPrint("Replica " + id + " received election message");
        var currentState = election.state;

        if (currentState.containsKey(id)) {
            // the ring has been completed
            Main.customPrint("Replica " + id + " ring completed");
            cancelRingElectionTimeout(); // Cancel the timeout because the ring is completed
            ActorRef nextCoordinator = null;
            Map.Entry<Integer, EpochSequencePair> mostRecentUpdate = null;
            Main.customPrint(
                    "Current state on Replica " + id + ": " + currentState);
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

            Main.customPrint(
                    "Replica's replicaMap " + replicasMap);

            if (nextCoordinator == null && (currentState.size() == replicasMap.size() + 1 || isForwardingMessage)) {
                isForwardingMessage = false;
                nextCoordinator = getSelf();
                // create the new coordinator with all the necesssary data
                onCrashForElection(new Crash(0));
                isCrashed = true;
                Main.customPrint("Replica " + id + " is the new coordinator");
                var newCoordinator = Main.system.actorOf(
                        Props.create(Coordinator.class, replicasMap.size() - 1,
                                new EpochSequencePair(mostRecentUpdate.getValue().epoch,
                                        mostRecentUpdate.getValue().seqNum)),
                        "coordinator" + (mostRecentUpdate.getValue().epoch + 1));

                for (var replica : replicasMap.entrySet()) {
                    Main.customPrint("Coordinator is adding " + replica.getKey());
                    newCoordinator.tell(new Register(replica.getKey()), replica.getValue());
                    replica.getValue().tell(new RegisterNewCoordinator(newCoordinator), getSelf());
                    Main.customPrint("Replica " + id + " sent sync message to the replica " + replica.getKey());
                    replica.getValue().tell(new Replica.Sync(crashedReplicas), getSelf());

                }

                Main.delay(2000);
                if (updateToComplete) {
                    Main.customPrint("New Coordinator is completing the update");
                    Main.customPrint("the Coordinator is sending update messages");
                    EpochSequencePair mostRecentUpdateValue = mostRecentUpdate.getValue();
                    replicasMap.entrySet().forEach(entry -> {
                        entry.getValue().tell(
                                new Replica.Update(mostRecentUpdateValue, history.get(mostRecentUpdateValue)),
                                getSelf());
                    });
                    updateToComplete = false;
                }
                Main.delay(100);
                newCoordinator.tell(new UpdateEpoch(mostRecentUpdate.getValue().epoch + 1), getSelf());
            } else {
                // just have to forward the message to the next replica
                isForwardingMessage = true;
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

        getSender().tell(new Ack(getSelf()), getSelf());
    }

    // Handler that is called when the new coordinator is elected
    private void onRegisterNewCoordinator(Replica.RegisterNewCoordinator registerNewCoordinator) {
        this.isNewCoordinatorElected = true;
        this.coordinator = registerNewCoordinator.newCoordinator;
        Main.customPrint("Replica " + id + " received the new coordinator " + coordinator);
    }

    // Handler that is called when a replica receives the SYNCRONIZATION message
    // during election
    private void onSync(Replica.Sync sync) {
        Main.customPrint("Replica " + id + " received the sync message");
        for (var id : sync.crashedReplicas) {
            replicasMap.remove(id);
        }
        getContext().become(createReceive());
    }

    // Handler that is called when a replica receives the ACK message during
    // election
    private void onAck(Replica.Ack msg) {
        if (underElection) {
            cancelElectionTimeout();
        }
    }

    // Handler that is called when a replica receives the info about all the other
    // replicas
    public void onAddReplica(AddReplicaRequest addReplica) {
        addReplica.replicasMap.remove(this.id);
        this.replicasMap = addReplica.replicasMap;
        Main.customPrint("Replica with id " + id + " is adding replicas:");
        for (Map.Entry<Integer, ActorRef> entry : replicasMap.entrySet()) {
            Main.customPrint("Replica with id " + entry.getKey() + " added for " + id);
        }
    }

    // Method the initiates the election process
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
        startRingElectionTimeout();
    }

    // Util method that gets the next replica info in a ring setting
    private Map.Entry<Integer, ActorRef> getNextReplica() {
        var replicasIds = new ArrayList<Integer>(replicasMap.keySet());
        int nextReplicaId = findNextGreater(replicasIds, id);
        ActorRef nextReplica = replicasMap.get(nextReplicaId);

        return new java.util.AbstractMap.SimpleEntry<Integer, ActorRef>(nextReplicaId, nextReplica);
    }

    // Method that fins the next replica id in a ring setting
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

    public static class UpdateEpoch {
        final int epoch;

        public UpdateEpoch(int epoch) {
            this.epoch = epoch;
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

    public static class RingElectionTimeout {
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
