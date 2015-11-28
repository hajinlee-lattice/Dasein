package com.latticeengines.pls.entitymanager.impl;

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
import com.latticeengines.security.exposed.service.TenantService;

public class TenantDeploymentEntityMgrTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private TenantDeploymentEntityMgr tenantDeploymentEntityMgr;

    @Autowired
    private TenantService tenantService;

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
        deployment.setModelId("modelid");
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
    public void findByTenantId() {
        Long tenantId = deployment.getTenantId();
        TenantDeployment retrivedDeployment = tenantDeploymentEntityMgr.findByTenantId(tenantId);
        Assert.assertNotNull(retrivedDeployment);
    }

}
