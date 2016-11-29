package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.monitor.exposed.metric.service.MetricService;

@Test
public class FuzzyMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String VALID_DUNS = "085576973";
    private static final String VALID_DOMAIN = "shell.com";
    private static final String EXPECTED_ID_DOMAIN_DUNS = "106254812";
    private static final String EXPECTED_ID_DOMAIN = "106254812";
    private static final String EXPECTED_ID_DUNS = "106254812";

    @Autowired
    private FuzzyMatchService service;

    @Autowired
    private MatchActorSystem actorSystem;

    @Autowired
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Autowired
    private MetricService metricService;

    @Test(groups = "pending", enabled = true)
    public void testSingleTraverse() throws Exception {
        LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.actors.exposed.traveler").setLevel(Level.DEBUG);
        metricService.enable();

        try {
            InternalOutputRecord matchRecord = new InternalOutputRecord();
            matchRecord.setParsedDuns(VALID_DUNS);
            matchRecord.setParsedDomain(VALID_DOMAIN);

            service.callMatch(Collections.singletonList(matchRecord), UUID.randomUUID().toString(),
                    dataCloudVersionEntityMgr.currentApprovedVersion().getVersion(), "Trilogy", Level.DEBUG, true);

            Assert.assertNotNull(matchRecord.getLatticeAccountId(), JsonUtils.serialize(matchRecord));
            Assert.assertEquals(matchRecord.getLatticeAccountId(), EXPECTED_ID_DOMAIN_DUNS);
        } finally {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // ignore
            }
            LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.INFO);
            LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.INFO);
            LogManager.getLogger("com.latticeengines.actors.exposed.traveler").setLevel(Level.INFO);
            actorSystem.setBatchMode(false);
            metricService.disable();
        }
    }

    @Test(groups = "functional", dataProvider = "actorTestData")
    public void testActorSystem(int numRequests, boolean batchMode) throws Exception {
        actorSystem.setBatchMode(batchMode);
        metricService.enable();

        if (!batchMode) {
            LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);
            LogManager.getLogger("com.latticeengines.datacloud.match.actors").setLevel(Level.DEBUG);
            LogManager.getLogger("com.latticeengines.actors.exposed.traveler").setLevel(Level.DEBUG);
        }

        try {
            List<OutputRecord> matchRecords = prepareData(numRequests);
            service.callMatch(matchRecords, UUID.randomUUID().toString(),
                    dataCloudVersionEntityMgr.currentApprovedVersion().getVersion(), MatchGuideBook.DEFAULT_GRAPH,
                    Level.DEBUG, true);

            boolean hasError = false;
            for (OutputRecord result : matchRecords) {
                Assert.assertNotNull(result);
                InternalOutputRecord record = (InternalOutputRecord) result;
                if (VALID_DUNS.equals(record.getParsedDuns()) || VALID_DOMAIN.equals(record.getParsedDomain())) {
                    try {
                        Assert.assertNotNull(record.getLatticeAccountId(), JsonUtils.serialize(record));
                        if (record.getParsedDuns() == null) {
                            Assert.assertEquals(record.getLatticeAccountId(), EXPECTED_ID_DOMAIN);
                        } else if (record.getParsedDomain() == null) {
                            Assert.assertEquals(record.getLatticeAccountId(), EXPECTED_ID_DUNS);
                        } else {
                            Assert.assertEquals(record.getLatticeAccountId(), EXPECTED_ID_DOMAIN_DUNS);
                        }
                    } catch (AssertionError e) {
                        System.out.println(e.getMessage());
                        hasError = true;
                    }
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

    @DataProvider(name = "actorTestData")
    public Object[][] provideActorTestData() {
        return new Object[][] {
                { 1000, true }, // 1000 match in batch mode
                { 100, false }, // 100 match in realtime mode
        };
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
