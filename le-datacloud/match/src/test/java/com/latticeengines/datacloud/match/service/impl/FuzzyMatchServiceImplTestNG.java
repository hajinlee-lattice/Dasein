package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

@Test
public class FuzzyMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Autowired
    private FuzzyMatchService service;

    @Test(groups = "pending")
    public void testActorSystem() throws Exception {
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
                    matchRecord.setParsedDuns("832433726");
                    matchRecord.setParsedDomain("co.wood.wi.us");
                }

                matchRecord.setParsedNameLocation(parsedNameLocation);
                matchRecords.add(matchRecord);
            }

            service.callMatch(matchRecords, "2.0.0");

            for (OutputRecord result : matchRecords) {
                Assert.assertNotNull(result);
                InternalOutputRecord matchRecord = (InternalOutputRecord) result;
                Assert.assertNotNull(matchRecord.getLatticeAccountId());
                System.out.println(matchRecord.getLatticeAccountId());
            }
        } finally {
            LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.INFO);
            LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.INFO);
        }
    }
}
