package com.latticeengines.dante.service.impl;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dante.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.dante.service.TalkingPointService;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class TalkingPointServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    TalkingPointService talkingPointService;

    @Autowired
    TalkingPointEntityMgr talkingPointEntityMgr;

    private final String externalID = "talkingPointFTestExtID";

    @BeforeClass(groups = "functional")
    public void setup() {
        DanteTalkingPoint dtp = talkingPointEntityMgr.findByExternalID(externalID);
        if (dtp != null)
            talkingPointEntityMgr.delete(dtp);
    }

    @Test(groups = "functional")
    public void testCreateFromService() {
        DanteTalkingPoint dtp = new DanteTalkingPoint();
        dtp.setCustomerID("test");
        dtp.setExternalID(externalID);
        dtp.setPlayExternalID("testFPlayExtID");
        dtp.setValue("Some Talking Point");

        talkingPointService.createOrUpdate(dtp);
        Assert.assertNotNull(dtp.getCreationDate(), "Failure Cause: Creation Date is NULL");
        Assert.assertNotNull(dtp.getLastModificationDate(), "Failure Cause: LastModificationDate is NULL");

        Date oldLastModificationDate = dtp.getLastModificationDate();

        dtp.setValue("New Talking Point Test");

        talkingPointService.createOrUpdate(dtp);

        dtp = talkingPointService.findByExternalID(externalID);
        Assert.assertNotNull(dtp,
                "Failure Cause: Talking Point not found by extrenal ID where externalID = " + externalID);
        Assert.assertEquals(dtp.getValue(), "New Talking Point Test", "Failure Cause: Talking Point value incorrect");

        Assert.assertNotEquals(dtp.getLastModificationDate(), oldLastModificationDate,
                "Failure Cause: Lastmodification date not updated by createOrUpdate()");

        List<DanteTalkingPoint> dtps = talkingPointService.findAllByPlayID("testFPlayExtID");

        Assert.assertEquals(dtps.size(), 1, "Failure Cause: Talking Points not found by findByPlayID");

        talkingPointEntityMgr.delete(dtp);

        dtp = talkingPointEntityMgr.findByField("External_ID", externalID);
        Assert.assertNull(dtp, "Failure Cause: Talking point was not deleted");
    }

}
