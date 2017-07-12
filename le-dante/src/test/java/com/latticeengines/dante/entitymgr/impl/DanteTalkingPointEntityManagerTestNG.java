package com.latticeengines.dante.entitymgr.impl;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class DanteTalkingPointEntityManagerTestNG extends AbstractTestNGSpringContextTests {

    // @Autowired
    // private DanteTalkingPointEntityMgr danteTalkingPointEntityMgr;
    //
    // private final String externalID = "talkingPointTestExtID";
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
    // public void testCreate() {
    //
    // DanteTalkingPoint dtp = new DanteTalkingPoint();
    // dtp.setCustomerID("test");
    // dtp.setExternalID(externalID);
    // dtp.setPlayExternalID("testPlayExtID");
    // dtp.setValue("Some Talking Point");
    //
    // danteTalkingPointEntityMgr.create(dtp);
    //
    // dtp = danteTalkingPointEntityMgr.findByExternalID(externalID);
    // Assert.assertNotNull(dtp);
    // Assert.assertNotNull(dtp.getCreationDate());
    // Assert.assertNotNull(dtp.getLastModificationDate());
    // Assert.assertEquals(dtp.getPlayExternalID(), "testPlayExtID");
    //
    // danteTalkingPointEntityMgr.delete(dtp);
    //
    // dtp = danteTalkingPointEntityMgr.findByField("External_ID", externalID);
    // Assert.assertNull(dtp);
    // }
}
