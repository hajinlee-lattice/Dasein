package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.PlayGroupEntityMgr;
import com.latticeengines.apps.cdl.service.PlayGroupService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.pls.PlayGroup;

public class PlayGroupFunctionalTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayGroupFunctionalTestNG.class);

    @Inject
    private PlayGroupService playGroupService;

    @Inject
    private PlayGroupEntityMgr playGroupEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCrud() throws InterruptedException {
        log.info("Start Test");
        PlayGroup newPlayGroup = new PlayGroup(mainTestTenant, "NewPlayGroup", "pliu@le.com", "pliu@le.com");
        playGroupEntityMgr.create(newPlayGroup);
        Thread.sleep(1000); // To ensure rds reader instance has the time to
                            // sync with the writer instance

        List<PlayGroup> groups = playGroupService.getAllPlayGroups(mainCustomerSpace);
        Assert.assertFalse(CollectionUtils.isEmpty(groups));
        newPlayGroup = groups.get(0);
        Assert.assertNotNull(newPlayGroup);
        Assert.assertEquals(newPlayGroup.getDisplayName(), "NewPlayGroup");

        newPlayGroup = playGroupEntityMgr.findById(newPlayGroup.getId());
        Assert.assertNotNull(newPlayGroup);
        Assert.assertEquals(newPlayGroup.getDisplayName(), "NewPlayGroup");

        newPlayGroup.setDisplayName("NewPlayGroup1");
        newPlayGroup.setUpdatedBy("pliu@le.com");

        playGroupEntityMgr.update(newPlayGroup);
        Thread.sleep(1000); // To ensure rds reader instance has the time to
                            // sync with the writer instance

        newPlayGroup = playGroupEntityMgr.findByPid(newPlayGroup.getPid());
        Assert.assertNotNull(newPlayGroup);
        Assert.assertEquals(newPlayGroup.getDisplayName(), "NewPlayGroup1");
        Assert.assertEquals(newPlayGroup.getUpdatedBy(), "pliu@le.com");

        playGroupEntityMgr.delete(newPlayGroup);
        Thread.sleep(1000); // To ensure rds reader instance has the time to
                            // sync with the writer instance

        newPlayGroup = playGroupEntityMgr.findByPid(newPlayGroup.getPid());
        Assert.assertNull(newPlayGroup);
        log.info("Done");
    }
}
