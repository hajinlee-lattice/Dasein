package com.latticeengines.pls.controller;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.domain.exposed.ulysses.CompanyProfileRequest;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class CompanyProfileResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @Test(groups = "deployment")
    public void testGetCompanyProfile() {
        String url = getRestAPIHostPort() + "/pls/companyprofiles/?enforceFuzzyMatch=true";
        CompanyProfileRequest request = new CompanyProfileRequest();
        request.getRecord().put("Email", "someuser@google.com");
        request.getRecord().put("Domain", "www.google.com");
        CompanyProfile profile = restTemplate.postForObject(url, request, CompanyProfile.class);
        assertNotNull(profile);
        assertNotNull(profile.getAttributes());
        assertFalse(MapUtils.isEmpty(profile.getAttributes()));
        assertNotNull(profile.getCompanyInfo());
        assertFalse(MapUtils.isEmpty(profile.getCompanyInfo()));
        assertNotNull(profile.getMatchLogs());
        assertFalse(CollectionUtils.isEmpty(profile.getMatchLogs()));

        for (String attr : profile.getAttributes().keySet()) {
            Object value = profile.getAttributes().get(attr);

            assertNotNull(value);
            assertFalse("Attr: " + attr, value.equals("null"));
        }

        for (String attr : profile.getCompanyInfo().keySet()) {
            Object value = profile.getCompanyInfo().get(attr);

            assertNotNull(value);
            assertFalse("Attr: " + attr, value.equals("null"));
        }
    }

    @Test(groups = "deployment")
    public void testNotAuthorized() {
        String url = getRestAPIHostPort() + "/pls/companyprofiles/";
        boolean thrown = false;
        CompanyProfileRequest request = new CompanyProfileRequest();
        request.getRecord().put("Email", "someuser@google.com");
        try {
            new RestTemplate().postForObject(url, request, CompanyProfile.class);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("401"));
            thrown = true;
        }
        assertTrue(thrown);
    }
}
