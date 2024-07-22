package it.ds1;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import akka.actor.typed.ActorRef;


public class ReplicaActorTest {
    static final ActorTestKit testKit = ActorTestKit.create();

    @BeforeClass
    public static void setup() {
    }

    @AfterClass
    public static void tearDown() {
        testKit.shutdownTestKit();
    }

    @Test
    public void replicaActorHandlesWriteRequests() {
        TestProbe<Void> probe = testKit.createTestProbe();
        ActorRef<ReplicaActor.Command> actor = testKit.spawn(ReplicaActor.create());
        ReplicaActor.UpdateId updateId = new ReplicaActor.UpdateId(1, 1);
        actor.tell(new ReplicaActor.WriteRequest("Hello, Akka!", updateId));
        
        // Since there's no direct reply to check in this test, we focus on the log output.
        // For more interactive testing, consider extending Actor behaviors to provide feedback.
    }
}
