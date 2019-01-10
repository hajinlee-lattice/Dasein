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
import com.latticeengines.datacloud.match.actors.framework.MatchDecisionGraphService;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityDomainCountryBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityDunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityIdAssociateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityNameCountryBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntitySystemIdBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.MatchPlannerMicroEngineActor;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

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

    private static final String DOMAIN_FIELD = "Domain";
    private static final String DUNS_FIELD = "DUNS";

    @Autowired
    private FuzzyMatchService service;

    @Autowired
    private MatchActorSystem actorSystem;

    @Autowired
    private MatchDecisionGraphService matchDecisionGraphService;

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
                    hasError = hasError || verifyTravelHistory(result.getMatchLogs(), expectedTravelStops);
                }

                // Verify match result
                InternalOutputRecord record = (InternalOutputRecord) result;
                if (decisionGraph.equals(ldcMatchDG)) {
                    hasError = hasError || verifyLDCMatchResult(record, expectedID);
                } else {
                    hasError = hasError || verifyEntityMatchResult(record);
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
                        "FuzzyMatchJunctionActor", //
                        DunsDomainBasedMicroEngineActor.class.getSimpleName(), //
                        EntityDunsBasedMicroEngineActor.class.getSimpleName(), //
                        EntityDomainCountryBasedMicroEngineActor.class.getSimpleName(), //
                        EntityNameCountryBasedMicroEngineActor.class.getSimpleName(), //
                        EntityIdAssociateMicroEngineActor.class.getSimpleName()), //
                        null, DOMAIN, DUNS }, //
                // Disble contact match test for now. More initialization work
                // to be done
                /*
                { 50, contactMatchDG, String.join(",", //
                        MatchPlannerMicroEngineActor.class.getSimpleName(), //
                        EntitySystemIdBasedMicroEngineActor.class.getSimpleName(), //
                        "AccountMatchJunctionActor", //
                        MatchPlannerMicroEngineActor.class.getSimpleName(), //
                        EntitySystemIdBasedMicroEngineActor.class.getSimpleName(), //
                        "FuzzyMatchJunctionActor",  //
                        DunsDomainBasedMicroEngineActor.class.getSimpleName(), //
                        EntityDunsBasedMicroEngineActor.class.getSimpleName(), //
                        EntityDomainCountryBasedMicroEngineActor.class.getSimpleName(), //
                        EntityNameCountryBasedMicroEngineActor.class.getSimpleName(), //
                        EntityIdAssociateMicroEngineActor.class.getSimpleName(), //
                        EntityIdResolveMicroEngineActor.class.getSimpleName(), //
                        EntityEmailBasedMicroEngineActor.class.getSimpleName(), //
                        EntityNameCountryBasedMicroEngineActor.class.getSimpleName(), //
                        EntityIdAssociateMicroEngineActor.class.getSimpleName()), //
                        null, DOMAIN, DUNS }, //
                        */
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
            log.error("Exception in verifing travel history", e);
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

    private boolean verifyEntityMatchResult(InternalOutputRecord record) {
        try {
            Assert.assertNotNull(record.getEntityId());
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
        matchInput.setTenant(new Tenant(this.getClass().getSimpleName()));
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

}
