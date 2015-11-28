package com.latticeengines.pls.service.impl;

import java.sql.Timestamp;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.TenantDeployment;
import com.latticeengines.domain.exposed.pls.TenantDeploymentStatus;
import com.latticeengines.domain.exposed.pls.TenantDeploymentStep;
import com.latticeengines.pls.entitymanager.TenantDeploymentEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.TenantDeploymentService;

public class TenantDeploymentServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private TenantDeploymentService tenantDeploymentService;

    @Autowired
    private TenantDeploymentEntityMgr tenantDeploymentEntityMgr;

    private TenantDeployment deployment;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        deployment = new TenantDeployment();
        deployment.setTenantId(-1L);
        deployment.setCreatedBy("functionalTester@lattice-engines.com");
        DateTime dt = new DateTime(DateTimeZone.UTC);
        deployment.setCreateTime(new Timestamp(dt.getMillis()));
        deployment.setStep(TenantDeploymentStep.ENRICH_DATA);
        deployment.setStatus(TenantDeploymentStatus.IN_PROGRESS);
        deployment.setCurrentLaunchId(123L);
        tenantDeploymentEntityMgr.create(deployment);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        if (deployment != null) {
            Long tenantId = deployment.getTenantId();
            TenantDeployment retrivedDeployment = tenantDeploymentEntityMgr.findByTenantId(tenantId);
            tenantDeploymentEntityMgr.delete(retrivedDeployment);
        }
    }

    @Test(groups = "functional")
    public void testUpdateTenantDeployment() {
        deployment.setStep(TenantDeploymentStep.BUILD_MODEL);
        deployment.setStatus(TenantDeploymentStatus.SUCCESS);
        deployment.setCurrentLaunchId(789L);
        deployment.setModelId("modelid");
        tenantDeploymentService.updateTenantDeployment(deployment);

        Long tenantId = deployment.getTenantId();
        TenantDeployment retrivedDeployment = tenantDeploymentEntityMgr.findByTenantId(tenantId);
        Assert.assertEquals(retrivedDeployment.getStep(), TenantDeploymentStep.BUILD_MODEL);
        Assert.assertEquals(retrivedDeployment.getStatus(), TenantDeploymentStatus.SUCCESS);
        Assert.assertEquals(retrivedDeployment.getCurrentLaunchId(), new Long(789));
        Assert.assertEquals(retrivedDeployment.getModelId(), "modelid");
    }

    @Test(groups = "functional")
    public void testIsDeploymentCompleted() {
        TenantDeployment dep = new TenantDeployment();
        dep.setStep(TenantDeploymentStep.PUBLISH_SCORES);
        dep.setStatus(TenantDeploymentStatus.SUCCESS);
        Assert.assertTrue(tenantDeploymentService.isDeploymentCompleted(dep));

        dep.setStep(TenantDeploymentStep.SCORE_LEADS);
        dep.setStatus(TenantDeploymentStatus.SUCCESS);
        Assert.assertFalse(tenantDeploymentService.isDeploymentCompleted(dep));
    }
}
