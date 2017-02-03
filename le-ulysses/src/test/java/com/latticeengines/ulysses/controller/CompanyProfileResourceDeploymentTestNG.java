package com.latticeengines.ulysses.controller;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class CompanyProfileResourceDeploymentTestNG extends UlyssesDeploymentTestNGBase {

    @Test(groups = "deployment")
    public void testGetCompanyProfileUsingOAuth() {
        String url = ulyssesHostPort + "/ulysses/companyprofiles/?enforceFuzzyMatch=true";
        Map<String, String> map = new HashMap<>();
        map.put("Email", "someuser@google.com");
        CompanyProfile profile = getOAuth2RestTemplate().postForObject(url, map, CompanyProfile.class);
        assertNotNull(profile);
    }

    @Test(groups = "deployment")
    public void testNotAuthorized() {
        String url = ulyssesHostPort + "/ulysses/companyprofiles/";
        boolean thrown = false;
        Map<String, String> map = new HashMap<>();
        map.put("Email", "someuser@google.com");
        try {
            getGlobalAuthRestTemplate().postForObject(url, map, CompanyProfile.class);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("401"));
            thrown = true;
        }
        assertTrue(thrown);
    }
}
