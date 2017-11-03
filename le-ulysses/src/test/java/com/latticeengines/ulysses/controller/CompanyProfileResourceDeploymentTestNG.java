package com.latticeengines.ulysses.controller;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.domain.exposed.ulysses.CompanyProfileRequest;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class CompanyProfileResourceDeploymentTestNG extends UlyssesDeploymentTestNGBase {

    @Test(groups = "deployment")
    public void testGetCompanyProfileUsingOAuth() {
        String url = ulyssesHostPort + "/ulysses/companyprofiles/?enforceFuzzyMatch=true";
        CompanyProfileRequest request = new CompanyProfileRequest();
        request.getRecord().put("Email", "someuser@google.com");
        request.getRecord().put("Domain", "www.google.com");
        CompanyProfile profile = getOAuth2RestTemplate().postForObject(url, request, CompanyProfile.class);
        assertNotNull(profile);
    }

    @Test(groups = "deployment")
    public void testNotAuthorized() {
        String url = ulyssesHostPort + "/ulysses/companyprofiles/";
        boolean thrown = false;
        CompanyProfileRequest request = new CompanyProfileRequest();
        request.getRecord().put("Email", "someuser@google.com");
        try {
            getGlobalAuthRestTemplate().postForObject(url, request, CompanyProfile.class);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("401"));
            thrown = true;
        }
        assertTrue(thrown);
    }

    @Test(groups = "deployment")
    public void testIgnoreProperties() {
        String url = ulyssesHostPort + "/ulysses/companyprofiles/?enforceFuzzyMatch=true";
        ObjectNode body = JsonUtils.createObjectNode();
        body.put("record", JsonUtils.createObjectNode());
        ObjectNode record = (ObjectNode) body.get("record");
        record.put("Email", "someuser@google.com");
        body.put("ignoreMe", "foo");
        CompanyProfile profile = getOAuth2RestTemplate().postForObject(url, body, CompanyProfile.class);
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
}
