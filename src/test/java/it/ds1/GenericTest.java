package it.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class GenericTest {
    static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        System.out.println("Actor system setup complete.");
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
        System.out.println("Actor system shutdown complete.");
    }

    @Test
    public void testReadRequest() {
        new TestKit(system) {{
            final ActorRef coordinator = system.actorOf(Coordinator.props(), "coordinator-read-" + System.currentTimeMillis());
            final ActorRef replica = system.actorOf(Replica.props(coordinator), "replica1-read-" + System.currentTimeMillis());
            final ActorRef client = getRef();
            System.out.println("Sending read request from client to replica.");

            replica.tell(new Replica.ReadRequest(client), client);

            expectMsgEquals(0);
            System.out.println("Read request test completed successfully.");
        }};
    }

    @Test
    public void testUpdateRequest() {
        new TestKit(system) {{
            final ActorRef coordinator = system.actorOf(Coordinator.props(), "coordinator-update-" + System.currentTimeMillis());
            final ActorRef replica1 = system.actorOf(Replica.props(coordinator), "replica1-update-" + System.currentTimeMillis());
            final ActorRef client = getRef();
            System.out.println("Sending update request from client to replica1 with new value 10.");

            replica1.tell(new Replica.UpdateRequest(client, 10), client);

            // Simulate the coordinator processing the update request and sending update messages to the replicas
            expectNoMessage();

            System.out.println("Update request test completed successfully.");
        }};
    }
}
