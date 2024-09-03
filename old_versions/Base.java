package it.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Cancellable;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

// Pair class to hold update entries <e, i>
class UpdateEntry implements Serializable {
    public final int e;
    public final int i;

    public UpdateEntry(int e, int i) {
        this.e = e;
        this.i = i;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UpdateEntry that = (UpdateEntry) o;

        if (e != that.e) return false;
        return i == that.i;
    }

    @Override
    public int hashCode() {
        int result = e;
        result = 31 * result + i;
        return result;
    }
}

// Message classes to define the protocol
class UpdateRequest implements Serializable {
    public final UpdateEntry updateEntry;

    public UpdateRequest(UpdateEntry updateEntry) {
        this.updateEntry = updateEntry;
    }
}

class ReadRequest implements Serializable {
    public final int key;

    public ReadRequest(int key) {
        this.key = key;
    }
}

class ReadResponse implements Serializable {
    public final int value;

    public ReadResponse(int value) {
        this.value = value;
    }
}

class TimeoutMessage implements Serializable {
    public final int id;

    public TimeoutMessage(int id) {
        this.id = id;
    }
}

// Actor that represents a replica in the system
class Replica extends AbstractActor {
    private final Map<UpdateEntry, Integer> dataStore = new HashMap<>();
    private final Map<Integer, Cancellable> scheduledTimeouts = new HashMap<>();

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(UpdateRequest.class, this::handleUpdateRequest)
                .match(ReadRequest.class, this::handleReadRequest)
                .match(TimeoutMessage.class, this::handleTimeout)
                .build();
    }

    // Handles update requests by adding them to the data store
    private void handleUpdateRequest(UpdateRequest updateRequest) {
        // Adding the update to the data store
        dataStore.put(updateRequest.updateEntry, updateRequest.updateEntry.i);
        System.out.println("Replica " + getSelf().path().name() + " received update: " + updateRequest.updateEntry.e + ", " + updateRequest.updateEntry.i);
    }

    // Handles read requests by returning the value from the data store
    private void handleReadRequest(ReadRequest readRequest) {
        Integer value = dataStore.get(new UpdateEntry(readRequest.key, 0)); // Dummy pair to get the key
        if (value != null) {
            getSender().tell(new ReadResponse(value), getSelf());
        } else {
            System.out.println("Replica " + getSelf().path().name() + " has no data for key: " + readRequest.key);
        }
    }

    // Handles timeout by performing necessary actions, such as logging or retrying
    private void handleTimeout(TimeoutMessage timeoutMessage) {
        System.out.println("Timeout occurred for message ID: " + timeoutMessage.id);
    }

    // Schedules a timeout message to be sent to this actor
    private void scheduleTimeout(int id, long delayMillis) {
        Cancellable timeout = getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(delayMillis, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutMessage(id),
                getContext().dispatcher(),
                getSelf()
        );

        // If a timeout with the same ID exists, cancel it before rescheduling
        if (scheduledTimeouts.containsKey(id)) {
            scheduledTimeouts.get(id).cancel();
        }

        scheduledTimeouts.put(id, timeout);
    }
}

// Actor that coordinates updates across all replicas
class Coordinator extends AbstractActor {
    private final ActorRef[] replicas;

    public Coordinator(ActorRef[] replicas) {
        this.replicas = replicas;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(UpdateRequest.class, this::handleUpdateRequest)
                .build();
    }

    // Handles update requests by broadcasting them to all replicas
    private void handleUpdateRequest(UpdateRequest updateRequest) {
        // Two-phase broadcast to all replicas
        for (ActorRef replica : replicas) {
            replica.tell(updateRequest, getSelf());
        }
        System.out.println("Coordinator broadcasted update: " + updateRequest.updateEntry.e + ", " + updateRequest.updateEntry.i);
    }
}

// Main class to test the implementation
public class Base {
    public static void main(String[] args) {
        // Create an ActorSystem
        ActorSystem system = ActorSystem.create("TwoPhaseBroadcastSystem");

        // Create replicas
        ActorRef replica1 = system.actorOf(Props.create(Replica.class), "Replica1");
        ActorRef replica2 = system.actorOf(Props.create(Replica.class), "Replica2");
        ActorRef replica3 = system.actorOf(Props.create(Replica.class), "Replica3");

        // Array of replicas
        ActorRef[] replicas = new ActorRef[]{replica1, replica2, replica3};

        // Create a coordinator
        ActorRef coordinator = system.actorOf(Props.create(Coordinator.class, replicas), "Coordinator");

        // Send update requests to the coordinator
        coordinator.tell(new UpdateRequest(new UpdateEntry(1, 100)), ActorRef.noSender());
        coordinator.tell(new UpdateRequest(new UpdateEntry(2, 200)), ActorRef.noSender());

        // Send read requests to replicas
        replica1.tell(new ReadRequest(1), ActorRef.noSender());
        replica2.tell(new ReadRequest(2), ActorRef.noSender());

        // Schedule a timeout as an example
        replica1.tell(new TimeoutMessage(1), ActorRef.noSender());
    }
}
