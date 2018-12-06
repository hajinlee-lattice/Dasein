package com.latticeengines.datacloud.match.actors;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.monitor.exposed.metric.service.MetricService;

@Test
public class MatchActorSystemTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MatchActorSystemTestNG.class);

    @Value("${datacloud.match.default.decision.graph}")
    private String ldcMatchDG;
    // TODO: Will change to default decision graph per entity in property file
    private String accountMatchDG = "PetitFour";
    private String contactMatchDG = "Cupcake";

    private static final String VALID_DUNS = "079942718";
    private static final String VALID_DOMAIN = "abc.xyz";
    private static final String EXPECTED_ID_DOMAIN_DUNS = "280002626626";
    private static final String EXPECTED_ID_DOMAIN = "280002626626";
    private static final String EXPECTED_ID_DUNS = "280002626626";

    @Autowired
    private FuzzyMatchService service;

    @Autowired
    private MatchActorSystem actorSystem;

    @Autowired
    private MetricService metricService;

    // Realtime and batch mode cannot run at same time. Must be prioritized
    @Test(groups = "functional", dataProvider = "actorSystemTestData", priority = 1)
    public void testActorSystemRealtimeMode(int numRequests, String decisionGraph) throws Exception {
        actorSystem.setBatchMode(false);

        LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.datacloud.match.actors").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.actors.exposed.traveler").setLevel(Level.DEBUG);

        testActorSystem(numRequests, decisionGraph);
    }

    @Test(groups = "functional", dataProvider = "actorSystemTestData", priority = 2, enabled = false)
    public void testActorSystemBatchMode(int numRequests, String decisionGraph) throws Exception {
        actorSystem.setBatchMode(true);
        testActorSystem(numRequests * 10, decisionGraph);
    }

    private void testActorSystem(int numRequests, String decisionGraph) throws Exception {
        metricService.disable();

        try {
            MatchInput input = prepareMatchInput(decisionGraph);
            List<OutputRecord> matchRecords = prepareData(numRequests);
            service.callMatch(matchRecords, input);

            boolean hasError = false;
            for (OutputRecord result : matchRecords) {
                Assert.assertNotNull(result);
                log.info("MatchTravelerHistory");
                log.info(String.join("\n", result.getMatchLogs()));

                // Verify match result
                InternalOutputRecord record = (InternalOutputRecord) result;
                if (decisionGraph.equals(ldcMatchDG)) {
                    hasError = verifyLDCMatchResult(record);
                }

            }
            Assert.assertFalse(hasError, "There are errors, see logs above.");
        } finally {
            LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.INFO);
            LogManager.getLogger("com.latticeengines.datacloud.match.actors").setLevel(Level.INFO);
            LogManager.getLogger("com.latticeengines.actors.exposed.traveler").setLevel(Level.INFO);
            actorSystem.setBatchMode(false);
            metricService.disable();
        }

    }

    // #Row, DecisionGraph
    @DataProvider(name = "actorSystemTestData", parallel = true)
    public Object[][] provideActorTestData() {
        return new Object[][] { //
                { 50, ldcMatchDG }, //
                // { 50, accountMatchDG }, //
                // { 50, contactMatchDG }, //
        };
    }
    
    

    private boolean verifyLDCMatchResult(InternalOutputRecord record) {
        try {
            if (VALID_DOMAIN.equals(record.getParsedDomain()) || VALID_DUNS.equals(record.getParsedDuns())) {
                Assert.assertNotNull(record.getLatticeAccountId(), JsonUtils.serialize(record));
                if (record.getParsedDuns() == null) {
                    Assert.assertEquals(record.getLatticeAccountId(), EXPECTED_ID_DOMAIN);
                } else if (record.getParsedDomain() == null) {
                    Assert.assertEquals(record.getLatticeAccountId(), EXPECTED_ID_DUNS);
                } else {
                    Assert.assertEquals(record.getLatticeAccountId(), EXPECTED_ID_DOMAIN_DUNS);
                }
            } else { // Input Name+Location are faked
                Assert.assertNull(record.getLatticeAccountId(), JsonUtils.serialize(record));
            }
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
        matchInput.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        matchInput.setRootOperationUid(UUID.randomUUID().toString());
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(false);
        return matchInput;
    }

    private List<OutputRecord> prepareData(int numRecords) {
        List<OutputRecord> matchRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {

            InternalOutputRecord matchRecord = new InternalOutputRecord();
            if (i % 2 == 0) {
                matchRecord.setParsedDuns(VALID_DUNS);
            }

            if (i % 3 == 0) {
                matchRecord.setParsedDomain(VALID_DOMAIN);
            }

            if (i % 5 == 0) {
                NameLocation parsedNameLocation = new NameLocation();
                parsedNameLocation.setName(UUID.randomUUID().toString());
                parsedNameLocation.setCountry(UUID.randomUUID().toString());
                parsedNameLocation.setState(UUID.randomUUID().toString());
                parsedNameLocation.setCity(UUID.randomUUID().toString());
                matchRecord.setParsedNameLocation(parsedNameLocation);
            }

            if (matchRecord.getParsedDomain() == null && matchRecord.getParsedDuns() == null) {
                matchRecord.setParsedDomain(UUID.randomUUID().toString());
            }

            matchRecords.add(matchRecord);
        }
        return matchRecords;
    }
}
