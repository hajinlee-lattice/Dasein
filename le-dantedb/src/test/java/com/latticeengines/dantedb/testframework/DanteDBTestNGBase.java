package com.latticeengines.dantedb.testframework;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.dantedb.exposed.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.domain.exposed.dantetalkingpoints.DanteTalkingPoint;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = "classpath:test-dantedb-context.xml")
public class DanteDBTestNGBase extends AbstractTestNGSpringContextTests {

    @Autowired
    TalkingPointEntityMgr entityMgr;

    @Test(groups = "functional")
    public void create() {
        String externalId = "testExtID";

        DanteTalkingPoint existed = entityMgr.findByExternalID(externalId);
        entityMgr.delete(existed);
        Assert.assertNull(entityMgr.findByExternalID(externalId));

        System.out.println("### START ###");
        DanteTalkingPoint dtp = new DanteTalkingPoint();
        dtp.setCustomerID("test");
        dtp.setExternalID("testExtID");
        dtp.setPlayExternalID("testPlayExtID");
        dtp.setCreationDate(new Date());
        dtp.setLastModificationDate(new Date());
        dtp.setValue("Some Talking Point");

        entityMgr.create(dtp);
        Assert.assertNotNull(entityMgr.findByExternalID(externalId));
    }
}