package com.latticeengines.datacloud.match.actors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchDecisionGraphService;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityDomainCountryBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityDunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityIdAssociateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityIdResolveMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityNameCountryBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntitySystemIdBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.MatchPlannerMicroEngineActor;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

@Test
public class MatchActorSystemTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MatchActorSystemTestNG.class);

    @Value("${datacloud.match.default.decision.graph}")
    private String ldcMatchDG;
    // TODO: Will change to default decision graph per entity in property file
    private String accountMatchDG = "PetitFour";

    private static final String DUNS = "079942718";
    private static final String DOMAIN = "abc.xyz";
    private static final String LATTICE_ID = "280002626626";

    private static final String DOMAIN_FIELD = "Domain";
    private static final String DUNS_FIELD = "DUNS";

    private static final String FUZZY_MATCH_JUNCTION_ACTOR = "FuzzyMatchJunctionActor";
    private static final String[] LDC_TRAVEL_STOPS = { //
            DunsDomainBasedMicroEngineActor.class.getSimpleName(), //
    };
    private static final String[] ACCOUNT_TRAVEL_STOPS = {
            MatchPlannerMicroEngineActor.class.getSimpleName(), //
            EntitySystemIdBasedMicroEngineActor.class.getSimpleName(), //
            FUZZY_MATCH_JUNCTION_ACTOR, //
            EntityDunsBasedMicroEngineActor.class.getSimpleName(), //
            EntityDomainCountryBasedMicroEngineActor.class.getSimpleName(), //
            EntityNameCountryBasedMicroEngineActor.class.getSimpleName(), //
            EntityIdAssociateMicroEngineActor.class.getSimpleName(), //
            EntityIdResolveMicroEngineActor.class.getSimpleName(), //
    };

    @Inject
    private FuzzyMatchService service;

    @Inject
    private MatchActorSystem actorSystem;

    @Inject
    private MatchDecisionGraphService matchDecisionGraphService;

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

    private static final Tenant TENANT = new Tenant(
            MatchActorSystemTestNG.class.getSimpleName() + UUID.randomUUID().toString());

    @BeforeClass(groups = "functional")
    public void init() {
        entityMatchConfigurationService.setIsAllocateMode(true);
    }

    // Realtime and batch mode cannot run at same time. Must be prioritized
    @Test(groups = "functional", dataProvider = "actorSystemTestData", priority = 1, enabled = true)
    public void testActorSystemRealtimeMode(int numRequests, String decisionGraph, String expectedID, String domain,
            String duns) throws Exception {
        actorSystem.setBatchMode(false);
        testActorSystem(numRequests, decisionGraph, expectedID, domain, duns);
    }

    @Test(groups = "functional", dataProvider = "actorSystemTestData", priority = 2, enabled = true)
    public void testActorSystemBatchMode(int numRequests, String decisionGraph, String expectedID, String domain,
            String duns) throws Exception {
        actorSystem.setBatchMode(true);
        testActorSystem(numRequests * 10, decisionGraph, expectedID, domain, duns);
    }

    private void testActorSystem(int numRequests, String decisionGraph, String expectedID, String domain, String duns)
            throws Exception {
        try {
            entityMatchVersionService.bumpVersion(EntityMatchEnvironment.STAGING, TENANT);
            Integer maxRetries = null;
            try {
                DecisionGraph dg = matchDecisionGraphService.getDecisionGraph(decisionGraph);
                maxRetries = dg.getRetries();
            } catch (ExecutionException e) {
                throw new RuntimeException("Fail to load decision graph " + decisionGraph, e);
            }
            maxRetries = maxRetries == null ? 1 : maxRetries;

            MatchInput input = prepareMatchInput(decisionGraph);
            List<OutputRecord> matchRecords = prepareData(numRequests, domain, duns);
            service.callMatch(matchRecords, input);

            boolean hasError = false;
            String expectedEntityId = null;
            for (OutputRecord result : matchRecords) {
                int retries = getTravelRetriesFromTravelHistory(decisionGraph, result.getMatchLogs());
                Assert.assertTrue(retries <= maxRetries);

                Queue<String> expectedTravelStops = parseTravelStops(decisionGraph, retries);
                Assert.assertNotNull(result);
                /* Uncomment these lines for troubleshooting
                log.info(String.format("Retries = %d, Expected Stops = %s", retries,
                        String.join(",", expectedTravelStops)));
                log.info("MatchTravelerHistory");
                log.info(String.join("\n", result.getMatchLogs()));
                */

                // Verify travel history
                hasError = hasError || verifyTravelHistory(result.getMatchLogs(), expectedTravelStops);

                // Verify match result
                InternalOutputRecord record = (InternalOutputRecord) result;
                if (decisionGraph.equals(ldcMatchDG)) {
                    hasError = hasError || verifyLDCMatchResult(record, expectedID);
                } else {
                    hasError = hasError || verifyEntityMatchResult(record, expectedEntityId);
                    // Expect all the records match to same entity id as input
                    // is same
                    if (record.getEntityId() != null) {
                        expectedEntityId = record.getEntityId();
                    }
                }
            }
            Assert.assertFalse(hasError, "There are errors, see logs above.");
        } finally {
            actorSystem.setBatchMode(false);
        }

    }

    // #Row, DecisionGraph, ExpectedID, Domain, Duns
    @DataProvider(name = "actorSystemTestData")
    public Object[][] provideActorTestData() {
        return new Object[][] { //
                { 50, ldcMatchDG, LATTICE_ID, DOMAIN, DUNS }, //
                { 50, accountMatchDG, null, DOMAIN, DUNS }, //
        };
    }

    private int getTravelRetriesFromTravelHistory(String decisionGraph, List<String> travelLogs) {
        int retries = 1;
        Pattern pattern = Pattern.compile("Start traveling in decision graph " + decisionGraph + " for (\\d+) times");
        for (String log : travelLogs) {
            Matcher matcher = pattern.matcher(log);
            if (matcher.find()) {
                int round = Integer.valueOf(matcher.group(1));
                if (round > retries) {
                    retries = round;
                }
            }
        }
        return retries;
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
            log.error("Exception in verifing travel history", e);
            return true;
        }
        return false;
    }

    private Queue<String> parseTravelStops(String decisionGraph, int retries) {
        List<String> travelStops = new ArrayList<>();
        if (ldcMatchDG.equals(decisionGraph)) {
            // LDC match decision graph does not retry
            travelStops.addAll(Arrays.asList(LDC_TRAVEL_STOPS));
        } else if (accountMatchDG.equals(decisionGraph)) {
            // Only testing AllocateId mode for entity match which has retries.
            // Lookup mode in entity match does not have retries
            if (retries == 1) {
                travelStops.addAll(getAccountTravelStopsSingleRun());
            } else {
                travelStops.addAll(getAccountTravelStopsMultiRunsFor1stRun());
                for (int i = 2; i <= retries - 1; i++) {
                    travelStops.addAll(getAccountTravelStopsMultiRunsNoId());
                }
                travelStops.addAll(getAccountTravelStopsMultiRunsWithId());

            }
        } else {
            throw new IllegalArgumentException("Unhandled decision graph " + decisionGraph);
        }
        return new LinkedList<>(travelStops);

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

    private boolean verifyEntityMatchResult(InternalOutputRecord record, String expectedEntityId) {
        try {
            if (expectedEntityId == null) {
                // expectedEntityId = null is not having expected entity id yet,
                // not expected to be null
                Assert.assertNotNull(record.getEntityId());
            } else {
                Assert.assertEquals(record.getEntityId(), expectedEntityId);
            }
        } catch (AssertionError e) {
            log.error("Exception in verifing entity match result", e);
            return true;
        }
        return false;
    }

    private MatchInput prepareMatchInput(String dgName) {
        MatchInput matchInput = new MatchInput();
        matchInput.setLogLevelEnum(Level.DEBUG);
        matchInput.setDecisionGraph(dgName);
        matchInput.setDataCloudVersion(currentDataCloudVersion);
        matchInput.setRootOperationUid(UUID.randomUUID().toString());
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(false);
        matchInput.setTenant(TENANT);
        matchInput.setAllocateId(true);
        matchInput.setEntityKeyMaps(prepareEntityKeyMap());
        matchInput.setFields(Arrays.asList(DOMAIN_FIELD, DUNS_FIELD));
        try {
            DecisionGraph dg = matchDecisionGraphService.getDecisionGraph(dgName);
            matchInput.setTargetEntity(dg.getEntity());
            // OperationalMode.CDL_LOOKUP case is not covered here
            if (matchInput.getTargetEntity().equals(BusinessEntity.LatticeAccount.name())) {
                matchInput.setOperationalMode(OperationalMode.LDC_MATCH);
            } else {
                matchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
            }
        } catch (ExecutionException e) {
            throw new RuntimeException("Fail to load decision graph " + dgName, e);
        }
        return matchInput;
    }

    private Map<String, EntityKeyMap> prepareEntityKeyMap() {
        Map<String, EntityKeyMap> entityKeyMaps = new HashMap<>();

        EntityKeyMap entityKeyMap = new EntityKeyMap();
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList(DOMAIN_FIELD));
        keyMap.put(MatchKey.DUNS, Collections.singletonList(DUNS_FIELD));
        entityKeyMap.setKeyMap(keyMap);
        entityKeyMaps.put(BusinessEntity.Account.name(), entityKeyMap);

        return entityKeyMaps;
    }

    private List<OutputRecord> prepareData(int numRecords, String domain, String duns) {
        List<OutputRecord> matchRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            InternalOutputRecord matchRecord = new InternalOutputRecord();
            matchRecord.setParsedDuns(duns);
            matchRecord.setParsedDomain(domain);
            // same order as matchInput.fields
            Object[] rawData = new Object[] { domain, duns };
            matchRecord.setInput(Arrays.asList(rawData));
            // set position according to matchInput.fields
            Map<MatchKey, List<Integer>> keyPosMap = new HashMap<>();
            keyPosMap.put(MatchKey.Domain, Arrays.asList(0));
            keyPosMap.put(MatchKey.DUNS, Arrays.asList(1));
            Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMaps = new HashMap<>();
            entityKeyPositionMaps.put(BusinessEntity.Account.name(), keyPosMap);
            matchRecord.setEntityKeyPositionMap(entityKeyPositionMaps);
            matchRecords.add(matchRecord);
        }
        return matchRecords;
    }

    // Only travel once
    private List<String> getAccountTravelStopsSingleRun() {
        List<String> travelStops = new ArrayList<>(Arrays.asList(ACCOUNT_TRAVEL_STOPS));
        // Inject travel stops for ldc match decision graph
        int fuzzyMatchJunctionIdx = travelStops.indexOf(FUZZY_MATCH_JUNCTION_ACTOR);
        travelStops.addAll(fuzzyMatchJunctionIdx + 1, Arrays.asList(LDC_TRAVEL_STOPS));
        // Remove last EntityIdResolveMicroEngineActor because
        // EntityIdAssociateMicroEngineActor already gets a match
        travelStops.remove(travelStops.size() - 1);
        return travelStops;
    }

    // If travel multiple times, for 1st run and no id associated
    private List<String> getAccountTravelStopsMultiRunsFor1stRun() {
        List<String> travelStops = new ArrayList<>(Arrays.asList(ACCOUNT_TRAVEL_STOPS));
        // Inject travel stops for ldc match decision graph
        int fuzzyMatchJunctionIdx = travelStops.indexOf(FUZZY_MATCH_JUNCTION_ACTOR);
        travelStops.addAll(fuzzyMatchJunctionIdx + 1, Arrays.asList(LDC_TRAVEL_STOPS));
        return travelStops;
    }

    // If travel multiple times, for non-1st run and no id associated
    private List<String> getAccountTravelStopsMultiRunsNoId() {
        return new ArrayList<>(Arrays.asList(ACCOUNT_TRAVEL_STOPS));
    }

    // If travel multiple times, for final run with id associated
    private List<String> getAccountTravelStopsMultiRunsWithId() {
        List<String> travelStops = new ArrayList<>(Arrays.asList(ACCOUNT_TRAVEL_STOPS));
        // Remove last EntityIdResolveMicroEngineActor because
        // EntityIdAssociateMicroEngineActor already gets a match
        travelStops.remove(travelStops.size() - 1);
        return travelStops;
    }
}
