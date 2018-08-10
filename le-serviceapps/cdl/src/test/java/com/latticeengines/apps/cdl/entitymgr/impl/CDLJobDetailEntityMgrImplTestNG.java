package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.CDLJobDetailEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public class CDLJobDetailEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    @Autowired
    private CDLJobDetailEntityMgr cdlJobDetailEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional")
    public void testCreateAndGet() {
        cdlJobDetailEntityMgr.createJobDetail(CDLJobType.PROCESSANALYZE, mainTestTenant);
        List<CDLJobDetail> cdlJobDetails = cdlJobDetailEntityMgr.listAllRunningJobByJobType(CDLJobType.PROCESSANALYZE);
        Assert.assertEquals(1, cdlJobDetails.size());
        cdlJobDetails.get(0).setCdlJobStatus(CDLJobStatus.COMPLETE);
        cdlJobDetails.get(0).setApplicationId("Fake_AppId");
        cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetails.get(0));
        CDLJobDetail cdlJobDetail = cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROCESSANALYZE);
        Assert.assertNotNull(cdlJobDetail);
        Assert.assertTrue(cdlJobDetail.getApplicationId().equals("Fake_AppId"));

    }
}
