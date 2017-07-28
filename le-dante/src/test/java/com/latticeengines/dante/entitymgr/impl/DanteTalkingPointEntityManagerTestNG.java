package com.latticeengines.dante.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dante.entitymgr.DanteTalkingPointEntityMgr;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class DanteTalkingPointEntityManagerTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private DanteTalkingPointEntityMgr danteTalkingPointEntityMgr;

    private final String externalID = "talkingPointTestExtID";

    @BeforeClass(groups = "functional")
    public void setup() {
        DanteTalkingPoint dtp = danteTalkingPointEntityMgr.findByExternalID(externalID);
        if (dtp != null)
            danteTalkingPointEntityMgr.delete(dtp);
    }

    @Test(groups = "functional")
    public void testCrud() {

        DanteTalkingPoint dtp = new DanteTalkingPoint();
        dtp.setCustomerID("test");
        dtp.setExternalID(externalID);
        dtp.setPlayExternalID("testPlayExtID");
        dtp.setValue("Some Talking Point");

        danteTalkingPointEntityMgr.create(dtp);

        dtp = danteTalkingPointEntityMgr.findByExternalID(externalID);
        Assert.assertNotNull(dtp);
        Assert.assertNotNull(dtp.getCreationDate());
        Assert.assertNotNull(dtp.getLastModificationDate());
        Assert.assertEquals(dtp.getPlayExternalID(), "testPlayExtID");

        danteTalkingPointEntityMgr.delete(dtp);

        dtp = danteTalkingPointEntityMgr.findByField("External_ID", externalID);
        Assert.assertNull(dtp);
    }
}
