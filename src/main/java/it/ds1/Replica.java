package it.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import scala.collection.immutable.List;
import scala.concurrent.duration.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import akka.actor.Cancellable;
import it.ds1.Replica.WriteOkTimeout;
import it.ds1.Replica.WriteRequestTimeout;

public class Replica extends AbstractActor {
    private int id;
    private int localValue;
    private EpochSequencePair lastUpdate;
    private ActorRef coordinator;
    private Map<EpochSequencePair, Integer> history = new HashMap<>();
    private ArrayList<ActorRef> replicas = new ArrayList<>();

    private Cancellable heartbeatTimer;
    private Cancellable writeOkTimeout;
    private Cancellable writeRequestTimeout;

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
                .match(Heartbeat.class, this::onHeartbeat)
                .match(Timeout.class, this::onTimeout)
                .match(WriteOkTimeout.class, this::onWriteOkTimeout)
                .match(WriteRequestTimeout.class, this::onWriteRequestTimeout)
                .match(Register.class, this::onRegister) // Aggiungi il gestore per il messaggio di registrazione
                .match(AddReplicaRequest.class, this::onAddReplica)
                .build();
    }

    // Metodo centralizzato per pianificare i timeout (come richiesto dal prof)
    private Cancellable scheduleTimeout(Cancellable currentTimeout, Object timeoutMessage, int delayInSeconds) {
        Main.customPrint("Scheduling timeout for " + timeoutMessage.getClass().getSimpleName() + " at Replica " + id);
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
        // Main.customPrint("Replica " + id + " registered with Coordinator.");
        resetHeartbeatTimer(); // Avvia il timer solo dopo che la replica è stata registrata
    }

    // Pianifica il timeout per il messaggio WRITEOK
    private void startWriteOkTimeout() {
        cancelWriteOkTimeout(); // Cancella il timeout precedente
        // writeOkTimeout = scheduleTimeout(writeOkTimeout, new WriteOkTimeout(), 5); //
        // Timeout di 5 secondi per aspettare il WRITEOK
    }

    // Pianifica il timeout per l'inoltro della richiesta di scrittura
    private void startWriteRequestTimeout() {
        cancelWriteRequestTimeout(); // Cancella il timeout precedente
        // writeRequestTimeout = scheduleTimeout(writeRequestTimeout, new
        // WriteRequestTimeout(), 5); // Timeout di 5 secondi per il broadcast
    }

    // Cancella il timeout per WRITEOK
    private void cancelWriteOkTimeout() {
        if (writeOkTimeout != null) {
            writeOkTimeout.cancel();
            writeOkTimeout = null;
        }
    }

    // Cancella il timeout per la richiesta di scrittura
    private void cancelWriteRequestTimeout() {
        if (writeRequestTimeout != null) {
            writeRequestTimeout.cancel();
            writeRequestTimeout = null;
        }
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
        Main.customPrint("Replica " + id + " applying update.");

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
            startWriteOkTimeout(); // Avvia il timeout per aspettare il WRITEOK
        }

    }

    // Metodo per reimpostare il timer del heartbeat
    private void resetHeartbeatTimer() {
        // heartbeatTimer = scheduleTimeout(heartbeatTimer, new Timeout(), 10); //
        // Timeout di 10 secondi per il messaggio heartbeat
    }

    // Gestistione del messaggio Heartbeat
    private void onHeartbeat(Heartbeat heartbeat) {
        Main.customPrint("Heartbeat received at Replica " + id);
        resetHeartbeatTimer(); // Reimposta il timer del heartbeat quando viene ricevuto un heartbeat
    }

    // Gestione del timeout quando il coordinatore non invia il WRITEOK
    private void onWriteOkTimeout(WriteOkTimeout msg) {
        Main.customPrint("Timeout while waiting for WRITEOK at Replica " + id + ": Coordinator crash suspected");
        // Gestione del crash o avvio di un'elezione
    }

    // Gestione del timeout quando il coordinatore non avvia il broadcast
    private void onWriteRequestTimeout(WriteRequestTimeout msg) {
        Main.customPrint("Timeout while waiting for coordinator to broadcast WRITE request at Replica " + id
                + ": Coordinator crash suspected");
        // Gestione del crash o avvio di un'elezione
    }

    // Gestione del timeout generale
    private void onTimeout(Timeout timeout) {
        Main.customPrint("Timeout detected at Replica " + id + ": Coordinator crash suspected");
        // Potenziale codice per avviare l'elezione o altre azioni
    }

    public int getId() {
        return this.id;
    }

    public void onAddReplica(AddReplicaRequest addReplica) {
        // addReplica
        getSelf().tell(addReplica, coordinator);
        Main.customPrint("Replica " + this + " with id " + id + " is adding replica " + addReplica.replica);
        this.replicas.add(addReplica.replica);
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

    public static class WriteOk {
    }

    public static class Register {
        final int id;

        public Register(int id) {
            this.id = id;
        }
    }

    public static class AddReplicaRequest {
        final ActorRef replica;

        public AddReplicaRequest(ActorRef replica) {
            this.replica = replica;
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
}
