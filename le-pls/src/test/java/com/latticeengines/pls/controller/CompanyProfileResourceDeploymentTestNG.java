package com.latticeengines.pls.controller;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class CompanyProfileResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @Test(groups = "deployment")
    public void testGetCompanyProfile() {
        String url = getRestAPIHostPort() + "/pls/companyprofiles/?Email=someuser@google.com";
        CompanyProfile profile = restTemplate.getForObject(url, CompanyProfile.class);
        assertNotNull(profile);
    }

    @Test(groups = "deployment")
    public void testNotAuthorized() {
        String url = getRestAPIHostPort() + "/pls/companyprofiles/?Email=someuser@google.com";
        boolean thrown = false;
        RestTemplate restTemplate = new RestTemplate();
        try {
            restTemplate.getForObject(url, CompanyProfile.class);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("401"));
            thrown = true;
        }
        assertTrue(thrown);
    }
}
