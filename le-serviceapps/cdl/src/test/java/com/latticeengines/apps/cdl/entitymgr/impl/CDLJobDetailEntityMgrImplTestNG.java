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
    public void testCreateAndGet() {
        jobDetail = cdlJobDetailEntityMgr.createJobDetail(CDLJobType.PROCESSANALYZE, mainTestTenant);
        List<CDLJobDetail> cdlJobDetails = cdlJobDetailEntityMgr.listAllRunningJobByJobType(CDLJobType.PROCESSANALYZE);
        Assert.assertEquals(CollectionUtils.size(cdlJobDetails), countBeforeTest + 1);
        cdlJobDetails.get(0).setCdlJobStatus(CDLJobStatus.COMPLETE);
        cdlJobDetails.get(0).setApplicationId("Fake_AppId");
        cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetails.get(0));
        CDLJobDetail cdlJobDetail = cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROCESSANALYZE);
        Assert.assertNotNull(cdlJobDetail);
        Assert.assertEquals(cdlJobDetail.getApplicationId(), "Fake_AppId");
    }
}
