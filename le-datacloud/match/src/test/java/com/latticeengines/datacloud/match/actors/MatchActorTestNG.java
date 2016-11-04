package com.latticeengines.datacloud.match.actors;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.impl.DnbLookupActor;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchOutput;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

@Component
public class MatchActorTestNG extends DataCloudMatchFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(MatchActorTestNG.class);

    @Autowired
    private MatchActorSystem actorSystem;

    @Test(groups = "functional")
    public void testDnBActor() throws Exception {
        DataSourceLookupRequest msg = new DataSourceLookupRequest();
        msg.setCallerMicroEngineReference(null);
        MatchKeyTuple matchKeyTuple = new MatchKeyTuple();
        matchKeyTuple.setCountryCode("US");
        matchKeyTuple.setState("CALIFORNIA");
        matchKeyTuple.setCity("FOSTER CITY");
        matchKeyTuple.setName("LATTICE ENGINES");
        msg.setInputData(matchKeyTuple);
        String rootOperationUid = UUID.randomUUID().toString();
        MatchTraveler matchTravelerContext = new MatchTraveler(rootOperationUid);
        msg.setMatchTravelerContext(matchTravelerContext);

        Response result = (Response) sendMessageToActor(msg, DnbLookupActor.class);
        Assert.assertNotNull(result);
        DnBMatchOutput data = (DnBMatchOutput) result.getResult();
        Assert.assertEquals(data.getDnbCode(), DnBReturnCode.OK);
        Assert.assertEquals(data.getDuns(), "028675958");
        Assert.assertEquals((int) data.getConfidenceCode(), 7);
        log.info(data.getMatchGrade());
    }

    private Object sendMessageToActor(Object msg, Class<? extends ActorTemplate> actorClazz) throws Exception {
        LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);

        ActorRef actorRef = actorSystem.getActorRef(actorClazz);

        Timeout timeout = new Timeout(new FiniteDuration(10, TimeUnit.MINUTES));
        Future<Object> future = Patterns.ask(actorRef, msg, timeout);

        Object result = Await.result(future, timeout.duration());
        return result;
    }
}
