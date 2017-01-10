package com.latticeengines.ulysses.controller;

import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class CompanyProfileResourceDeploymentTestNG extends UlyssesDeploymentTestNGBase {

    @Override
    public CustomerSpace getCustomerSpace() {
        return CustomerSpace.parse("CompanyProfileResourceDeploymentTestNG");
    }

    @Test(groups = "deployment", enabled = true)
    public void testGetCompanyProfile() {
        String url = ulyssesHostPort + "/ulysses/companyprofiles/?Email=someuser@google.com";
        CompanyProfile profile = oAuth2RestTemplate.getForObject(url, CompanyProfile.class);
        assertNull(profile);
    }
}
