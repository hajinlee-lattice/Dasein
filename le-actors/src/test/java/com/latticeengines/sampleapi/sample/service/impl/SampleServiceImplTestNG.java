package com.latticeengines.sampleapi.sample.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;

import com.latticeengines.actors.visitor.sample.framework.SampleMatchActorSystem;
import com.latticeengines.actors.visitor.sample.impl.SampleDnbLookupActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDynamoLookupActor;
import com.latticeengines.sampleapi.sample.service.SampleFuzzyMatchService;
import com.latticeengines.sampleapi.sample.service.SampleInternalOutputRecord;
import com.latticeengines.sampleapi.sample.service.SampleNameLocation;
import com.latticeengines.sampleapi.sample.service.SampleOutputRecord;

@ContextConfiguration(locations = { "classpath:test-sample-service-context.xml" })
public class SampleServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private SampleFuzzyMatchService service;

    // @Test(groups = "functional")
    public void testActorSystem() throws Exception {
        LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);

        try {
            Thread.sleep(3000);
            Assert.assertEquals(SampleMatchActorSystem.ACTOR_CARDINALITY,
                    SampleDnbLookupActor.actorCardinalityCounterForTest.get());
            Assert.assertEquals(SampleMatchActorSystem.ACTOR_CARDINALITY,
                    SampleDynamoLookupActor.actorCardinalityCounterForTest.get());

            List<SampleOutputRecord> matchRecords = new ArrayList<>();
            int MAX = 1;
            for (int i = 0; i < MAX; i++) {

                SampleInternalOutputRecord matchRecord = new SampleInternalOutputRecord();
                SampleNameLocation parsedNameLocation = new SampleNameLocation();
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

            for (SampleOutputRecord result : matchRecords) {
                Assert.assertNotNull(result);
                SampleInternalOutputRecord matchRecord = (SampleInternalOutputRecord) result;
                Assert.assertNotNull(matchRecord.getLatticeAccountId());
                System.out.println(matchRecord.getLatticeAccountId());
            }

            long timerCallCounterForTestSampleDnbLookupActor1 = SampleDnbLookupActor.timerCallCounterForTest.get();
            long timerCallCounterForTestSampleDynamoLookupActor1 = SampleDynamoLookupActor.timerCallCounterForTest
                    .get();

            Assert.assertNotEquals(timerCallCounterForTestSampleDnbLookupActor1, 0L);
            Assert.assertNotEquals(timerCallCounterForTestSampleDynamoLookupActor1, 0L);

            Thread.sleep(2000);

            long timerCallCounterForTestSampleDnbLookupActor2 = SampleDnbLookupActor.timerCallCounterForTest.get();
            long timerCallCounterForTestSampleDynamoLookupActor2 = SampleDynamoLookupActor.timerCallCounterForTest
                    .get();

            Assert.assertNotEquals(timerCallCounterForTestSampleDnbLookupActor2, 0L);
            Assert.assertNotEquals(timerCallCounterForTestSampleDynamoLookupActor2, 0L);

            Assert.assertNotEquals(timerCallCounterForTestSampleDnbLookupActor1,
                    timerCallCounterForTestSampleDnbLookupActor2);
            Assert.assertNotEquals(timerCallCounterForTestSampleDynamoLookupActor1,
                    timerCallCounterForTestSampleDynamoLookupActor2);
        } finally {
            LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.INFO);
            LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.INFO);
        }
    }
}
