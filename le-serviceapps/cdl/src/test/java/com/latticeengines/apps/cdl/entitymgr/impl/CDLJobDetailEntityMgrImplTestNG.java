package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.CDLJobDetailEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public class CDLJobDetailEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private CDLJobDetailEntityMgr cdlJobDetailEntityMgr;

    private CDLJobDetail jobDetail;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        if (jobDetail != null) {
            cdlJobDetailEntityMgr.delete(jobDetail);
        }
    }

    @Test(groups = "functional")
    public void testCreateAndGet() {
        List<CDLJobDetail> cdlJobDetails = cdlJobDetailEntityMgr
                .listAllRunningJobByJobType(CDLJobType.PROCESSANALYZE);
        int initCount = CollectionUtils.size(cdlJobDetails);
        jobDetail = cdlJobDetailEntityMgr.createJobDetail(CDLJobType.PROCESSANALYZE,
                mainTestTenant);
        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);
        cdlJobDetails = retry.execute(context -> {
            List<CDLJobDetail> newDetails = cdlJobDetailEntityMgr
                    .listAllRunningJobByJobType(CDLJobType.PROCESSANALYZE);
            Assert.assertNotNull(newDetails);
            Assert.assertTrue(newDetails.size() >= initCount + 1, //
                    "There should be at least " + (initCount + 1) + " CDL Job Details.");
            return newDetails;
        });
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
        retry.execute(context -> {
            CDLJobDetail cdlJobDetail = cdlJobDetailEntityMgr
                    .findLatestJobByJobType(CDLJobType.PROCESSANALYZE);
            Assert.assertNotNull(cdlJobDetail);
            Assert.assertEquals(cdlJobDetail.getApplicationId(), "Fake_AppId");
            return true;
        });
    }
}
