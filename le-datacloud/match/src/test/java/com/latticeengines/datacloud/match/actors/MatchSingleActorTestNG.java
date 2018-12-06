package com.latticeengines.datacloud.match.actors;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.impl.DnbLookupActor;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

@Component
public class MatchSingleActorTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MatchSingleActorTestNG.class);

    // IMPORTANT
    // Should not run tests in parallel which need different mode
    // (batch/realtime) for actorSystem
    @Autowired
    private MatchActorSystem actorSystem;

    @Test(groups = { "functional" }, dataProvider = "dnbLookupActorData", priority = 1)
    public void testDnBActorRealtimeMode(String name, String countryCode, String state, String city,
            DnBReturnCode dnbCode, String expectedDuns, int expectedConfidenceCode, String expectedMatchGrade)
            throws Exception {
        // Realtime mode, use DnBCache
        testDnBActor(name, countryCode, state, city, dnbCode, expectedDuns, expectedConfidenceCode, expectedMatchGrade,
                false, true);
    }

    // Submit to remote DnB batch API, so set test group as dnb which only runs
    // once a week
    @Test(groups = "dnb", dataProvider = "dnbLookupActorData", priority = 2)
    public void testDnBActorBatchMode(String name, String countryCode, String state, String city, DnBReturnCode dnbCode,
            String expectedDuns, int expectedConfidenceCode, String expectedMatchGrade) throws Exception {
        // Batch mode, skip DnBCache
        testDnBActor(name, countryCode, state, city, dnbCode, expectedDuns, expectedConfidenceCode, expectedMatchGrade,
                true, false);
    }

    private void testDnBActor(String name, String countryCode, String state, String city, DnBReturnCode dnbCode,
            String expectedDuns, int expectedConfidenceCode, String expectedMatchGrade, boolean isBatchMode,
            boolean useDnBCache) throws Exception {
        actorSystem.setBatchMode(isBatchMode);
        DataSourceLookupRequest msg = new DataSourceLookupRequest();
        msg.setCallerMicroEngineReference(null);
        MatchKeyTuple.Builder tupleBuilder = new MatchKeyTuple.Builder();
        MatchKeyTuple matchKeyTuple = tupleBuilder.withCountryCode(countryCode) //
                .withState(state) //
                .withCity(city) //
                .withName(name) //
                .build();
        msg.setInputData(matchKeyTuple);
        String rootOperationUid = UUID.randomUUID().toString();
        MatchTraveler matchTravelerContext = new MatchTraveler(rootOperationUid, matchKeyTuple);
        MatchInput matchInput = new MatchInput(); // Just set minimum fields
                                                  // needed to DnBLookupActor
        matchInput.setUseDnBCache(useDnBCache);
        matchInput.setUseRemoteDnB(Boolean.TRUE);
        matchInput.setDataCloudVersion(currentDataCloudVersion);
        matchTravelerContext.setMatchInput(matchInput);
        msg.setMatchTravelerContext(matchTravelerContext);

        Response result = (Response) sendMessageToActor(msg, DnbLookupActor.class, false);
        Assert.assertNotNull(result);
        DnBMatchContext data = (DnBMatchContext) result.getResult();
        log.info(String.format("DnBReturnCode = %s, DUNS = %s, ConfidenceCode = %d, MatchGrade = %s",
                data.getDnbCodeAsString(), data.getDuns(), data.getConfidenceCode(),
                data.getMatchGrade().getRawCode()));
        Assert.assertEquals(data.getDnbCode(), dnbCode);
        Assert.assertEquals(data.getDuns(), expectedDuns);
        Assert.assertEquals((int) data.getConfidenceCode(), expectedConfidenceCode);
        Assert.assertEquals(data.getMatchGrade().getRawCode(), expectedMatchGrade);
    }

    // Name, CountryCode, State, City, DnBReturnCode, ExpectedDuns,
    // ExpectedConfidenceCode, ExpectedMatchGrade
    @DataProvider(name = "dnbLookupActorData")
    private Object[][] provideDnBLookupData() {
        return new Object[][] { { "LATTICE ENGINES", "US", "CALIFORNIA", "FOSTER CITY", DnBReturnCode.OK, "028675958",
                7, "AZZFAZZAFAF" }, //
        };
    }

    private Object sendMessageToActor(Object msg, Class<? extends ActorTemplate> actorClazz, boolean batchMode)
            throws Exception {
        LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);

        ActorRef actorRef = actorSystem.getActorRef(actorClazz);

        Timeout timeout = batchMode ? new Timeout(new FiniteDuration(30, TimeUnit.MINUTES))
                : new Timeout(new FiniteDuration(10, TimeUnit.MINUTES));
        Future<Object> future = Patterns.ask(actorRef, msg, timeout);

        Object result = Await.result(future, timeout.duration());
        return result;
    }
}
