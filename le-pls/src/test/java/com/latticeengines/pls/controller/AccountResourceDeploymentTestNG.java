package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;

public class AccountResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @Test(groups = "deployment", enabled = false)
    public void checkEnvironment() {
        DataCollection collection = dataCollectionProxy.getDataCollectionByType(mainTestTenant.getId(),
                DataCollectionType.Segmentation);
        assertNotNull(collection);
        assertEquals(collection.getTables().size(), 1);
    }

    @Test(groups = "deployment", enabled = false)
    public void testGetCount() {
        FrontEndQuery query = new FrontEndQuery();
        try (PerformanceTimer timer = new PerformanceTimer("testGetCount")) {
            long count = restTemplate.postForObject(getRestAPIHostPort() + "/pls/accounts/count", query, Long.class);
            assertTrue(count > 0);
        }
    }
}
