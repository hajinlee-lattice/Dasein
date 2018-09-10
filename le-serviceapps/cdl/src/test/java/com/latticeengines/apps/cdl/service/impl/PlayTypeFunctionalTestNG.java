package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.PlayTypeEntityMgr;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.pls.PlayType;

public class PlayTypeFunctionalTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayTypeFunctionalTestNG.class);

    @Inject
    private PlayTypeService playTypeService;

    @Inject
    private PlayTypeEntityMgr playTypeEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCreateDefaultsAndGet() {
        Assert.assertTrue(CollectionUtils.isEmpty(playTypeEntityMgr.findAll()));
        List<PlayType> types = playTypeService.getAllPlayTypes(mainCustomerSpace);
        Assert.assertFalse(CollectionUtils.isEmpty(types));
        Assert.assertEquals(types.size(), 5);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateDefaultsAndGet" })
    public void testCrud() {
        PlayType newPlayType = new PlayType(mainTestTenant, "NewPlayType", "ToTest", "jlm@le.com", "jlm@le.com");
        playTypeEntityMgr.create(newPlayType);

        newPlayType = playTypeEntityMgr.findById(newPlayType.getId());
        Assert.assertNotNull(newPlayType);
        Assert.assertEquals(newPlayType.getDisplayName(), "NewPlayType");

        newPlayType.setDisplayName("NewPlayType1");
        newPlayType.setUpdatedBy("jm@le.com");

        playTypeEntityMgr.update(newPlayType);
        newPlayType = playTypeEntityMgr.findByPid(newPlayType.getPid());
        Assert.assertNotNull(newPlayType);
        Assert.assertEquals(newPlayType.getDisplayName(), "NewPlayType1");
        Assert.assertEquals(newPlayType.getUpdatedBy(), "jm@le.com");

        playTypeEntityMgr.delete(newPlayType);
        newPlayType = playTypeEntityMgr.findByPid(newPlayType.getPid());
        Assert.assertNull(newPlayType);
    }
}
