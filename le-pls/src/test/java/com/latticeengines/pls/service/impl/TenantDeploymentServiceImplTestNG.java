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
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.TenantDeploymentService;

public class TenantDeploymentServiceImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    @Autowired
    private TenantDeploymentService tenantDeploymentService;

    @Autowired
    private TenantDeploymentEntityMgr tenantDeploymentEntityMgr;

    private TenantDeployment deployment;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        deployment = newDeployment(-1L);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        if (deployment != null) {
            tenantDeploymentEntityMgr.deleteByTenantId(deployment.getTenantId());
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
    public void testDeleteTenantDeployment() {
        TenantDeployment dep = newDeployment(-2L);
        Assert.assertNotNull(tenantDeploymentEntityMgr.findByTenantId(dep.getTenantId()));
        tenantDeploymentEntityMgr.deleteByTenantId(dep.getTenantId());
        Assert.assertNull(tenantDeploymentEntityMgr.findByTenantId(dep.getTenantId()));
        Assert.assertNotNull(tenantDeploymentEntityMgr.findByTenantId(deployment.getTenantId()));
    }

    @Test(groups = "functional")
    public void testIsDeploymentCompleted() {
        TenantDeployment dep = new TenantDeployment();
        dep.setStep(TenantDeploymentStep.VALIDATE_METADATA);
        dep.setStatus(TenantDeploymentStatus.SUCCESS);
        Assert.assertTrue(tenantDeploymentService.isDeploymentCompleted(dep));

        dep.setStep(TenantDeploymentStep.ENRICH_DATA);
        dep.setStatus(TenantDeploymentStatus.SUCCESS);
        Assert.assertFalse(tenantDeploymentService.isDeploymentCompleted(dep));
    }

    private TenantDeployment newDeployment(long tenantId) {
        TenantDeployment deployment = new TenantDeployment();
        deployment.setTenantId(tenantId);
        deployment.setCreatedBy("functionalTester@lattice-engines.com");
        DateTime dt = new DateTime(DateTimeZone.UTC);
        deployment.setCreateTime(new Timestamp(dt.getMillis()));
        deployment.setStep(TenantDeploymentStep.ENRICH_DATA);
        deployment.setStatus(TenantDeploymentStatus.IN_PROGRESS);
        deployment.setCurrentLaunchId(123L);
        tenantDeploymentEntityMgr.create(deployment);
        return deployment;
    }
}
