package com.latticeengines.dante.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dante.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class TalkingPointEntityManagerTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    TalkingPointEntityMgr talkingPointEntityMgr;

    private final String externalID = "talkingPointTestExtID";

    @BeforeClass(groups = "functional")
    public void setup() {
        DanteTalkingPoint dtp = talkingPointEntityMgr.findByExternalID(externalID);
        if (dtp != null)
            talkingPointEntityMgr.delete(dtp);
    }

    @Test(groups = "functional")
    public void testCreate() {

        DanteTalkingPoint dtp = new DanteTalkingPoint();
        dtp.setCustomerID("test");
        dtp.setExternalID(externalID);
        dtp.setPlayExternalID("testPlayExtID");
        dtp.setValue("Some Talking Point");

        talkingPointEntityMgr.create(dtp);

        dtp = null;

        dtp = talkingPointEntityMgr.findByExternalID(externalID);
        Assert.assertNotNull(dtp);
        Assert.assertNotNull(dtp.getCreationDate());
        Assert.assertNotNull(dtp.getLastModificationDate());
        Assert.assertEquals(dtp.getPlayExternalID(), "testPlayExtID");

        talkingPointEntityMgr.delete(dtp);

        dtp = talkingPointEntityMgr.findByField("External_ID", externalID);
        Assert.assertNull(dtp);
    }
}
