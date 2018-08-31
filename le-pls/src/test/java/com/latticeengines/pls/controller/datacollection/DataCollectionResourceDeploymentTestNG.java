package com.latticeengines.pls.controller.datacollection;

import javax.inject.Inject;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.serviceapps.cdl.DataCollectionPrechecks;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class DataCollectionResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DataCollectionResourceDeploymentTestNG.class);

    @Inject
    private CDLTestDataService cdlTestDataService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "deployment")
    public void testPrecheck() {
        DataCollectionPrechecks prechecks = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/datacollection/precheck", DataCollectionPrechecks.class);
        Assert.assertNotNull(prechecks);
        Assert.assertTrue(prechecks.getDisableAllCuratedMetrics());
        Assert.assertTrue(prechecks.getDisableShareOfWallet());
        Assert.assertTrue(prechecks.getDisableMargin());
        Assert.assertTrue(prechecks.getDisableCrossSellModeling());

        cdlTestDataService.populateData(mainTestTenant.getId(), 3);
        prechecks = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/datacollection/precheck", DataCollectionPrechecks.class);
        Assert.assertNotNull(prechecks);
        Assert.assertTrue(prechecks.getDisableAllCuratedMetrics());
        Assert.assertTrue(prechecks.getDisableShareOfWallet());
        Assert.assertTrue(prechecks.getDisableMargin());
        Assert.assertTrue(prechecks.getDisableCrossSellModeling());
    }
}
