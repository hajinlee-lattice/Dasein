package com.latticeengines.dante.service.impl;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class TalkingPointServiceImplTestNG extends AbstractTestNGSpringContextTests {

    // @Autowired
    // private TalkingPointService talkingPointService;
    //
    // @Autowired
    // private DanteTalkingPointEntityMgr danteTalkingPointEntityMgr;
    //
    // private final String externalID = "talkingPointFTestExtID";
    //
    // @BeforeClass(groups = "functional")
    // public void setup() {
    // DanteTalkingPoint dtp =
    // danteTalkingPointEntityMgr.findByExternalID(externalID);
    // if (dtp != null)
    // danteTalkingPointEntityMgr.delete(dtp);
    // }
    //
    // @Test(groups = "functional")
    // public void testCreateFromService() {
    // List<DanteTalkingPoint> dtps = new ArrayList<>();
    //
    // DanteTalkingPoint dtp = new DanteTalkingPoint();
    // dtp.setCustomerID("test");
    // dtp.setExternalID(externalID);
    // dtp.setPlayExternalID("testFPlayExtID");
    // dtp.setValue("Some Talking Point");
    //
    // dtps.add(dtp);
    //
    // talkingPointService.createOrUpdate(dtps);
    // Assert.assertNotNull(dtp.getCreationDate(), "Failure Cause: Creation Date
    // is NULL");
    // Assert.assertNotNull(dtp.getLastModificationDate(), "Failure Cause:
    // LastModificationDate is NULL");
    //
    // Date oldLastModificationDate = dtp.getLastModificationDate();
    // dtp.setValue("New Talking Point Test");
    //
    // talkingPointService.createOrUpdate(dtps);
    //
    // dtp = talkingPointService.findByExternalID(externalID);
    // Assert.assertNotNull(dtp,
    // "Failure Cause: Talking Point not found by extrenal ID where externalID =
    // " + externalID);
    // Assert.assertEquals(dtp.getValue(), "New Talking Point Test", "Failure
    // Cause: Talking Point value incorrect");
    // Assert.assertNotEquals(dtp.getLastModificationDate(),
    // oldLastModificationDate,
    // "Failure Cause: Lastmodification date not updated by createOrUpdate()");
    //
    // dtps = talkingPointService.findAllByPlayID("testFPlayExtID");
    //
    // Assert.assertEquals(dtps.size(), 1, "Failure Cause: Talking Points not
    // found by findByPlayID");
    //
    // danteTalkingPointEntityMgr.delete(dtp);
    //
    // dtp = danteTalkingPointEntityMgr.findByField("External_ID", externalID);
    // Assert.assertNull(dtp, "Failure Cause: Talking point was not deleted");
    // }
}
