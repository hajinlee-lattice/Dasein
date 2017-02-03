package com.latticeengines.pls.controller;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.HashMap;
import java.util.Map;

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
    public void testGetCompanyProfileUsingOAuth() {
        String url = getRestAPIHostPort() + "/ulysses/companyprofiles/?enforceFuzzyMatch=true";
        Map<String, String> map = new HashMap<>();
        map.put("Email", "someuser@google.com");
        CompanyProfile profile = restTemplate.postForObject(url, map, CompanyProfile.class);
        assertNotNull(profile);
    }

    @Test(groups = "deployment")
    public void testNotAuthorized() {
        String url = getRestAPIHostPort() + "/ulysses/companyprofiles/";
        boolean thrown = false;
        Map<String, String> map = new HashMap<>();
        map.put("Email", "someuser@google.com");
        try {
            restTemplate.postForObject(url, map, CompanyProfile.class);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("401"));
            thrown = true;
        }
        assertTrue(thrown);
    }
}
