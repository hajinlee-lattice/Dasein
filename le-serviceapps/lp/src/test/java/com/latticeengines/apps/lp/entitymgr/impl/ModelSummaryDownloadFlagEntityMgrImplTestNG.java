package com.latticeengines.apps.lp.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.entitymgr.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;

public class ModelSummaryDownloadFlagEntityMgrImplTestNG extends LPFunctionalTestNGBase {

    @Inject
    private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCURLExcludeFlag() throws InterruptedException {
        modelSummaryDownloadFlagEntityMgr.addExcludeFlag(mainTestTenant.getId());
        modelSummaryDownloadFlagEntityMgr.addExcludeFlag("tenantID_1");
        modelSummaryDownloadFlagEntityMgr.addExcludeFlag(mainTestTenant.getId());

        List<String> tenantIdList = modelSummaryDownloadFlagEntityMgr.getExcludeFlags();
        Assert.assertNotNull(tenantIdList);
        Assert.assertEquals(tenantIdList.size(), 2);

        Thread.sleep(500);
        modelSummaryDownloadFlagEntityMgr.removeExcludeFlag("tenantID_1");
        modelSummaryDownloadFlagEntityMgr.removeExcludeFlag(mainTestTenant.getId());
        tenantIdList = modelSummaryDownloadFlagEntityMgr.getExcludeFlags();
        Assert.assertNull(tenantIdList);
    }
}
