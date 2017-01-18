package com.latticeengines.ulysses.controller;

import static org.testng.AssertJUnit.assertNotNull;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class CompanyProfileResourceDeploymentTestNG extends UlyssesDeploymentTestNGBase {

    @Test(groups = "deployment")
    public void testGetCompanyProfileUsingOAuth() {
        String url = ulyssesHostPort + "/ulysses/companyprofiles/?Email=someuser@google.com";
        CompanyProfile profile = getOAuth2RestTemplate().getForObject(url, CompanyProfile.class);
        assertNotNull(profile);
    }
}
