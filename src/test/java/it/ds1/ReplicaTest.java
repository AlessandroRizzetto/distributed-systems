package it.ds1;

import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import org.junit.Test;
import akka.actor.ActorSystem;

public class ReplicaTest {
    @Test
    public void testUpdateHandling() {
        ActorSystem system = ActorSystem.create("testSystem");
        TestKit probe = new TestKit(system);
        ActorRef coordinator = system.actorOf(Coordinator.props(), "coordinatorTest");
        ActorRef replica = system.actorOf(Replica.props(coordinator), "replica");

        int newValue = 45;
        EpochSequencePair pair = new EpochSequencePair(1, 1);
        Replica.UpdateMessage updateMessage = new Replica.UpdateMessage(pair, newValue);
        
        replica.tell(updateMessage, coordinator);
        
        Replica.WriteOkMessage writeOkMessage = new Replica.WriteOkMessage(pair);
        replica.tell(writeOkMessage, coordinator);
        
        probe.watch(replica);
        replica.tell(new Replica.ReadRequest(probe.getRef()), probe.getRef());
        int receivedValue = probe.expectMsgClass(Integer.class);
        
        System.out.println("Received value: " + receivedValue + " Expected: " + newValue);
        assert receivedValue == newValue : "Value updated incorrectly in Replica.";
        
        TestKit.shutdownActorSystem(system);
    }
}
