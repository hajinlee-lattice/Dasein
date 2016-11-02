package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

@Test
public class FuzzyMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String VALID_DUNS = "832433726";
    private static final String VALID_DOMAIN = "co.wood.wi.us";
    private static final String EXPECTED_ID_DOMAIN_DUNS = "50310468";
    private static final String EXPECTED_ID_DOMAIN = "63550008";
    private static final String EXPECTED_ID_DUNS = "50310468";

    @Autowired
    private FuzzyMatchService service;

    @Autowired
    private MatchActorSystem actorSystem;

    @Autowired
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Test(groups = "pending", dataProvider = "actorTestData")
    public void testRealTimeActorSystem(int numRequests, boolean batchMode) throws Exception {
        actorSystem.setBatchMode(batchMode);

        if (!batchMode) {
            LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.DEBUG);
            LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);
        }

        try {
            List<OutputRecord> matchRecords = prepareData(numRequests);
            service.callMatch(matchRecords, UUID.randomUUID().toString(),
                    dataCloudVersionEntityMgr.currentApprovedVersion().getVersion());

            for (OutputRecord result : matchRecords) {
                Assert.assertNotNull(result);
                InternalOutputRecord record = (InternalOutputRecord) result;
                if (VALID_DUNS.equals(record.getParsedDuns()) || VALID_DOMAIN.equals(record.getParsedDomain())) {
                    Assert.assertNotNull(record.getLatticeAccountId());
                    if (record.getParsedDuns() == null) {
                        Assert.assertEquals(record.getLatticeAccountId(), EXPECTED_ID_DOMAIN);
                    } else if (record.getParsedDomain() == null) {
                        Assert.assertEquals(record.getLatticeAccountId(), EXPECTED_ID_DUNS);
                    } else {
                        Assert.assertEquals(record.getLatticeAccountId(), EXPECTED_ID_DOMAIN_DUNS);
                    }
                }
            }
        } finally {
            LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.INFO);
            LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.INFO);
            actorSystem.setBatchMode(false);
        }
    }

    @DataProvider(name = "actorTestData")
    public Object[][] provideActorTestData() {
        return new Object[][] {
                { 20, false },   // 20 match in realtime mode
                { 100, true }    // 100 match in batch mode
        };
    }

    private List<OutputRecord> prepareData(int numRecords) {
        List<OutputRecord> matchRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {

            InternalOutputRecord matchRecord = new InternalOutputRecord();
            NameLocation parsedNameLocation = new NameLocation();
            parsedNameLocation.setName(UUID.randomUUID().toString());
            parsedNameLocation.setCountry(UUID.randomUUID().toString());
            parsedNameLocation.setState(UUID.randomUUID().toString());

            if (i % 2 == 0) {
                parsedNameLocation.setCity(UUID.randomUUID().toString());
                matchRecord.setParsedDuns(VALID_DUNS);
            }

            if (i % 3 == 0) {
                matchRecord.setParsedDomain(VALID_DOMAIN);
            }

            if (matchRecord.getParsedDomain() == null && matchRecord.getParsedDuns() == null) {
                matchRecord.setParsedDomain(UUID.randomUUID().toString());
            }

            matchRecord.setParsedNameLocation(parsedNameLocation);
            matchRecords.add(matchRecord);
        }
        return matchRecords;
    }
}
