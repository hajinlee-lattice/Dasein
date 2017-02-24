package com.latticeengines.pls.controller;

import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class AccountResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
    }

    @Test(groups = "deployment")
    public void testGetCount() {
        FrontEndQuery query = new FrontEndQuery();
        long count = restTemplate.postForObject(getRestAPIHostPort() + "/pls/accounts/count", query, Long.class);
        assertTrue(count > 0);
    }
}
