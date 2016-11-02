package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

@Test
public class FuzzyMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String VALID_DUNS = "832433726";
    private static final String VALID_DOMAIN = "co.wood.wi.us";
    private static final String EXPECTED_LATTICE_ID = "50310468";

    @Autowired
    private FuzzyMatchService service;

    @Autowired
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Test(groups = "pending")
    public void testRealTimeActorSystem() throws Exception {
        LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);

        try {
            List<OutputRecord> matchRecords = new ArrayList<>();
            int MAX = 50;
            for (int i = 0; i < MAX; i++) {

                InternalOutputRecord matchRecord = new InternalOutputRecord();
                NameLocation parsedNameLocation = new NameLocation();
                parsedNameLocation.setName(UUID.randomUUID().toString());
                parsedNameLocation.setCountry(UUID.randomUUID().toString());
                parsedNameLocation.setState(UUID.randomUUID().toString());

                matchRecord.setParsedDomain(UUID.randomUUID().toString());
                if (i % 2 != 1) {
                    parsedNameLocation.setCity(UUID.randomUUID().toString());
                    matchRecord.setParsedDuns(VALID_DUNS);
                    matchRecord.setParsedDomain(VALID_DOMAIN);
                }

                matchRecord.setParsedNameLocation(parsedNameLocation);
                matchRecords.add(matchRecord);
            }

            service.callMatch(matchRecords, UUID.randomUUID().toString(),
                    dataCloudVersionEntityMgr.currentApprovedVersion().getVersion());

            for (OutputRecord result : matchRecords) {
                Assert.assertNotNull(result);
                InternalOutputRecord record = (InternalOutputRecord) result;
                if (VALID_DUNS.equals(record.getParsedDuns()) || VALID_DOMAIN.equals(record.getParsedDomain())) {
                    Assert.assertNotNull(record.getLatticeAccountId());
                    Assert.assertEquals(record.getLatticeAccountId(), EXPECTED_LATTICE_ID);
                }
            }
        } finally {
            LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.INFO);
            LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.INFO);
        }
    }
}
