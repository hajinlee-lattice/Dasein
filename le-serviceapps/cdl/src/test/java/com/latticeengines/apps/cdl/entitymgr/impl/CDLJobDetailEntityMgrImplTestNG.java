package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.CDLJobDetailEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public class CDLJobDetailEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private CDLJobDetailEntityMgr cdlJobDetailEntityMgr;

    private int countBeforeTest;
    private CDLJobDetail jobDetail;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
        List<CDLJobDetail> cdlJobDetails = cdlJobDetailEntityMgr.listAllRunningJobByJobType(CDLJobType.PROCESSANALYZE);
        countBeforeTest = CollectionUtils.size(cdlJobDetails);
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        if (jobDetail != null) {
            cdlJobDetailEntityMgr.delete(jobDetail);
        }
    }

    @Test(groups = "functional")
    public void testCreateAndGet() throws InterruptedException {
        jobDetail = cdlJobDetailEntityMgr.createJobDetail(CDLJobType.PROCESSANALYZE, mainTestTenant);
        Thread.sleep(500);
        List<CDLJobDetail> cdlJobDetails = cdlJobDetailEntityMgr.listAllRunningJobByJobType(CDLJobType.PROCESSANALYZE);
        Assert.assertNotNull(cdlJobDetails);
        Assert.assertTrue(cdlJobDetails.size() > 0);
        CDLJobDetail current = null;
        for (CDLJobDetail cdlJobDetail : cdlJobDetails) {
            if (cdlJobDetail.getTenantId().equals(mainTestTenant.getPid())) {
                current = cdlJobDetail;
            }
        }
        Assert.assertNotNull(current);
        current.setCdlJobStatus(CDLJobStatus.COMPLETE);
        current.setApplicationId("Fake_AppId");
        cdlJobDetailEntityMgr.updateJobDetail(current);
        CDLJobDetail cdlJobDetail = cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROCESSANALYZE);
        Assert.assertNotNull(cdlJobDetail);
        Assert.assertEquals(cdlJobDetail.getApplicationId(), "Fake_AppId");
    }
}
