package com.latticeengines.datacloud.match.actors;

import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.impl.DnbLookupActor;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;

import akka.actor.ActorRef;

public class MatchActorTestNG extends DataCloudMatchFunctionalTestNGBase {
    @Autowired
    private SampleActorSystem sampleActorSystem;

    @Test
    public void testDnBActor() {
        LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);

        ActorRef actorRef = sampleActorSystem.getActorRef(DnbLookupActor.class);
        DataSourceLookupRequest msg = new DataSourceLookupRequest();
        msg.setCallerMicroEngineReference(null);
        msg.setInputData("");
        String rootOperationUid = UUID.randomUUID().toString();
        MatchTraveler matchTravelerContext = new MatchTraveler(rootOperationUid);
        msg.setMatchTravelerContext(matchTravelerContext);
        actorRef.tell(msg, null);
    }
}
