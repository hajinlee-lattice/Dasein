package com.latticeengines.datacloud.match.actors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.impl.AccountMatchJunctionActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityDunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityEmailBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityIdAssociateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityNameBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntitySystemIdBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.FuzzyMatchJunctionActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.MatchPlannerMicroEngineActor;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

@Test
public class MatchActorSystemTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MatchActorSystemTestNG.class);

    @Value("${datacloud.match.default.decision.graph}")
    private String ldcMatchDG;
    // TODO: Will change to default decision graph per entity in property file
    private String accountMatchDG = "PetitFour";
    private String contactMatchDG = "Cupcake";

    private static final String DUNS = "079942718";
    private static final String DOMAIN = "abc.xyz";
    private static final String LATTICE_ID = "280002626626";

    @Autowired
    private FuzzyMatchService service;

    @Autowired
    private MatchActorSystem actorSystem;

    // Realtime and batch mode cannot run at same time. Must be prioritized
    @Test(groups = "functional", dataProvider = "actorSystemTestData", priority = 1)
    public void testActorSystemRealtimeMode(int numRequests, String decisionGraph, String expectedTravelStopStr,
            String expectedID, String domain, String duns) throws Exception {
        actorSystem.setBatchMode(false);

        LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.datacloud.match.actors").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.actors.exposed.traveler").setLevel(Level.DEBUG);

        testActorSystem(numRequests, decisionGraph, expectedTravelStopStr, expectedID, domain, duns);
    }

    @Test(groups = "functional", dataProvider = "actorSystemTestData", priority = 2, enabled = false)
    public void testActorSystemBatchMode(int numRequests, String decisionGraph, String expectedTravelStopStr,
            String expectedID, String domain, String duns) throws Exception {
        actorSystem.setBatchMode(true);
        testActorSystem(numRequests * 10, decisionGraph, null, expectedID, domain, duns);
    }

    private void testActorSystem(int numRequests, String decisionGraph, String expectedTravelStopStr, String expectedID,
            String domain, String duns) throws Exception {
        try {
            MatchInput input = prepareMatchInput(decisionGraph);
            List<OutputRecord> matchRecords = prepareData(numRequests, domain, duns);
            service.callMatch(matchRecords, input);

            boolean hasError = false;
            for (OutputRecord result : matchRecords) {
                Queue<String> expectedTravelStops = parseTravelStops(expectedTravelStopStr);
                Assert.assertNotNull(result);
                log.info("MatchTravelerHistory");
                log.info(String.join("\n", result.getMatchLogs()));

                // Verify travel history
                if (expectedTravelStopStr != null) {
                    hasError = verifyTravelHistory(result.getMatchLogs(), expectedTravelStops);
                }

                // Verify match result
                InternalOutputRecord record = (InternalOutputRecord) result;
                if (decisionGraph.equals(ldcMatchDG)) {
                    hasError = verifyLDCMatchResult(record, expectedID);
                }

            }
            Assert.assertFalse(hasError, "There are errors, see logs above.");
        } finally {
            LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.INFO);
            LogManager.getLogger("com.latticeengines.datacloud.match.actors").setLevel(Level.INFO);
            LogManager.getLogger("com.latticeengines.actors.exposed.traveler").setLevel(Level.INFO);
            actorSystem.setBatchMode(false);
        }

    }

    // #Row, DecisionGraph, ExpectedTravelHistory, ExpectedID, Domain, Duns
    @DataProvider(name = "actorSystemTestData")
    public Object[][] provideActorTestData() {
        return new Object[][] { //
                { 50, ldcMatchDG, DunsDomainBasedMicroEngineActor.class.getSimpleName(), LATTICE_ID, DOMAIN, DUNS }, //
                { 50, accountMatchDG, String.join(",", //
                        MatchPlannerMicroEngineActor.class.getSimpleName(), //
                        EntitySystemIdBasedMicroEngineActor.class.getSimpleName(), //
                        FuzzyMatchJunctionActor.class.getSimpleName(), //
                        DunsDomainBasedMicroEngineActor.class.getSimpleName(), //
                        EntityDunsBasedMicroEngineActor.class.getSimpleName(), //
                        EntityDomainBasedMicroEngineActor.class.getSimpleName(), //
                        EntityNameBasedMicroEngineActor.class.getSimpleName(), //
                        EntityIdAssociateMicroEngineActor.class.getSimpleName()), //
                        null, DOMAIN, DUNS }, //
                { 50, contactMatchDG, String.join(",", //
                        MatchPlannerMicroEngineActor.class.getSimpleName(), //
                        EntitySystemIdBasedMicroEngineActor.class.getSimpleName(), //
                        AccountMatchJunctionActor.class.getSimpleName(), //
                        MatchPlannerMicroEngineActor.class.getSimpleName(), //
                        EntitySystemIdBasedMicroEngineActor.class.getSimpleName(), //
                        FuzzyMatchJunctionActor.class.getSimpleName(), //
                        DunsDomainBasedMicroEngineActor.class.getSimpleName(), //
                        EntityDunsBasedMicroEngineActor.class.getSimpleName(), //
                        EntityDomainBasedMicroEngineActor.class.getSimpleName(), //
                        EntityNameBasedMicroEngineActor.class.getSimpleName(), //
                        EntityIdAssociateMicroEngineActor.class.getSimpleName(), //
                        EntityEmailBasedMicroEngineActor.class.getSimpleName(), //
                        EntityNameBasedMicroEngineActor.class.getSimpleName(), //
                        EntityIdAssociateMicroEngineActor.class.getSimpleName()), //
                        null, DOMAIN, DUNS }, //
        };
    }

    // TODO: Should have full travel stops in traveler instead of parsing
    // traveler log (Currently stop history only has)
    private boolean verifyTravelHistory(List<String> travelLogs, Queue<String> expectedTravelStops) {
        try {
            Pattern pattern = Pattern.compile("Arrived (.*?)\\.");
            travelLogs.forEach(log -> {
                Matcher matcher = pattern.matcher(log);
                if (matcher.find()) {
                    Assert.assertEquals(matcher.group(1), expectedTravelStops.peek());
                    expectedTravelStops.poll();
                }
            });
            Assert.assertTrue(expectedTravelStops.isEmpty());
        } catch (AssertionError e) {
            log.error("Exception in verifing LDC match result", e);
            return true;
        }
        return false;
    }

    private Queue<String> parseTravelStops(String travelStops) {
        if (travelStops == null) {
            return null;
        }
        String[] stops = travelStops.split(",");
        Queue<String> parsedTravelStops = new LinkedList<>(Arrays.asList(stops));
        return parsedTravelStops;
    }

    private boolean verifyLDCMatchResult(InternalOutputRecord record, String expectedID) {
        try {
            Assert.assertEquals(record.getLatticeAccountId(), expectedID);
        } catch (AssertionError e) {
            log.error("Exception in verifing LDC match result", e);
            return true;
        }
        return false;
    }

    private MatchInput prepareMatchInput(String decisionGraph) {
        MatchInput matchInput = new MatchInput();
        matchInput.setLogLevelEnum(Level.DEBUG);
        matchInput.setDecisionGraph(decisionGraph);
        matchInput.setDataCloudVersion(currentDataCloudVersion);
        matchInput.setRootOperationUid(UUID.randomUUID().toString());
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(false);
        return matchInput;
    }

    private List<OutputRecord> prepareData(int numRecords, String domain, String duns) {
        List<OutputRecord> matchRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            InternalOutputRecord matchRecord = new InternalOutputRecord();
            matchRecord.setParsedDuns(duns);
            matchRecord.setParsedDomain(domain);
            matchRecords.add(matchRecord);
        }
        return matchRecords;
    }

}
