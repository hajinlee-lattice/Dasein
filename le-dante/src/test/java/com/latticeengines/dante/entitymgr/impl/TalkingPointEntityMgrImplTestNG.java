package com.latticeengines.dante.entitymgr.impl;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class TalkingPointEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    // @Autowired
    // private TalkingPointEntityMgr talkingPointEntityMgr;
    //
    // private final String testName = "talkingPointTestExtID";
    // private Play testplay;
    //
    // @BeforeClass(groups = "functional")
    // public void setup() {
    // //create Play
    //
    //
    // TalkingPoint tp = talkingPointEntityMgr.findByField("name", testName);
    // if (tp != null)
    // talkingPointEntityMgr.delete(tp);
    // }
    //
    // @Test(groups = "functional")
    // public void testCreate() {
    //
    // TalkingPoint tp = new TalkingPoint();
    // tp.setPlayId();
    // tp.setExternalID(externalID);
    // tp.setTitle("Title 1");
    // tp.setContent("Some Talking Point");
    //
    // talkingPointEntityMgr.create(tp);
    //
    // tp = TalkingPointEntityMgr.findByExternalID(externalID);
    // Assert.assertNotNull(tp);
    // Assert.assertNotNull(tp.getCreationDate());
    // Assert.assertNotNull(tp.getLastModificationDate());
    // Assert.assertEquals(tp.getPlayExternalID(), "testPlayExtID");
    //
    // talkingPointEntityMgr.delete(tp);
    //
    // tp = talkingPointEntityMgr.findByField("External_ID", externalID);
    // Assert.assertNull(tp);
    // }
}
