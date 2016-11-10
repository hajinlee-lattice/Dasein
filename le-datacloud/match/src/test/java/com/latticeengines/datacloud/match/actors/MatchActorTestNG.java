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
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

@Component
public class MatchActorTestNG extends DataCloudMatchFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(MatchActorTestNG.class);

    private static final String COUNTRY_CODE = "US";
    private static final String STATE = "CALIFORNIA";
    private static final String CITY = "FOSTER CITY";
    private static final String NAME = "LATTICE ENGINES";
    private static final String DUNS = "028675958";
    private static final int CONFIDENCE_CODE = 7;
    private static final String MATCH_GRADE = "AZZFAZZAFAF";

    @Autowired
    private MatchActorSystem actorSystem;

    @Test(groups = "functional")
    public void testDnBActorNonBatchMode() throws Exception {
        actorSystem.setBatchMode(false);
        DataSourceLookupRequest msg = new DataSourceLookupRequest();
        msg.setCallerMicroEngineReference(null);
        MatchKeyTuple matchKeyTuple = new MatchKeyTuple();
        matchKeyTuple.setCountryCode(COUNTRY_CODE);
        matchKeyTuple.setState(STATE);
        matchKeyTuple.setCity(CITY);
        matchKeyTuple.setName(NAME);
        msg.setInputData(matchKeyTuple);
        String rootOperationUid = UUID.randomUUID().toString();
        MatchTraveler matchTravelerContext = new MatchTraveler(rootOperationUid, matchKeyTuple);
        msg.setMatchTravelerContext(matchTravelerContext);

        Response result = (Response) sendMessageToActor(msg, DnbLookupActor.class);
        Assert.assertNotNull(result);
        DnBMatchContext data = (DnBMatchContext) result.getResult();
        Assert.assertEquals(data.getDnbCode(), DnBReturnCode.OK);
        Assert.assertEquals(data.getDuns(), DUNS);
        Assert.assertEquals((int) data.getConfidenceCode(), CONFIDENCE_CODE);
        Assert.assertEquals(data.getMatchGrade().getRawCode(), MATCH_GRADE);
    }

    @Test(groups = "functional", dependsOnMethods = { "testDnBActorNonBatchMode" })
    public void testDnBActorBatchMode() throws Exception {
        actorSystem.setBatchMode(true);
        DataSourceLookupRequest msg = new DataSourceLookupRequest();
        msg.setCallerMicroEngineReference(null);
        MatchKeyTuple matchKeyTuple = new MatchKeyTuple();
        matchKeyTuple.setCountryCode(COUNTRY_CODE);
        matchKeyTuple.setState(STATE);
        matchKeyTuple.setCity(CITY);
        matchKeyTuple.setName(NAME);
        msg.setInputData(matchKeyTuple);
        String rootOperationUid = UUID.randomUUID().toString();
        MatchTraveler matchTravelerContext = new MatchTraveler(rootOperationUid, matchKeyTuple);
        msg.setMatchTravelerContext(matchTravelerContext);

        Response result = (Response) sendMessageToActor(msg, DnbLookupActor.class);
        Assert.assertNotNull(result);
        DnBMatchContext data = (DnBMatchContext) result.getResult();
        Assert.assertEquals(data.getDnbCode(), DnBReturnCode.OK);
        Assert.assertEquals(data.getDuns(), DUNS);
        Assert.assertEquals((int) data.getConfidenceCode(), CONFIDENCE_CODE);
        Assert.assertEquals(data.getMatchGrade().getRawCode(), MATCH_GRADE);
        actorSystem.setBatchMode(false);
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
