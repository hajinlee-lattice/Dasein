package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;

@Component
public class PlayTypeResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PlayTypeResourceDeploymentTestNG.class);

    @Inject
    private PlayProxy playProxy;

    private Tenant tenant;
    private PlayType playType;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        tenant = testBed.getMainTestTenant();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment")
    public void testCreateAndUpdate() {
        playType = new PlayType(tenant, "List", "Description", "admin.le.com", "admin.le.com");
        playProxy.createPlayType(tenant.getId(), playType);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment")
    public void deletePlayType() {
        playProxy.deletePlayTypeById(tenant.getId(), playType.getId());

    }
}
