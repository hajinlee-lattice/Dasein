package com.latticeengines.pls.controller;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.domain.exposed.ulysses.CompanyProfileRequest;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class CompanyProfileResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CompanyProfileResourceDeploymentTestNG.class);

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String featureFlag = LatticeFeatureFlag.LATTICE_INSIGHTS.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, flags);
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

        String[] requiredAttrs = { "LDC_Name", "LDC_Street", "LDC_City", "LDC_State", "LDC_ZipCode", "LDC_Domain",
                "LDC_DUNS", "LE_NUMBER_OF_LOCATIONS", "LE_IS_PRIMARY_LOCATION", "LE_INDUSTRY", "LE_REVENUE_RANGE",
                "LE_EMPLOYEE_RANGE" };
        Set<String> requiredAttrSet = new HashSet<String>(Arrays.asList(requiredAttrs));
        Set<String> actualAttrSet = new HashSet<>();

        for (String attr : profile.getCompanyInfo().keySet()) {
            Object value = profile.getCompanyInfo().get(attr);

            assertNotNull(value);
            assertFalse("Attr: " + attr, value.equals("null"));
            log.info(attr + ": " + value.toString());
            if (requiredAttrSet.contains(attr)) {
                actualAttrSet.add(attr);
            }
        }
        Assert.assertEquals(actualAttrSet.size(), requiredAttrSet.size());
    }

    @Test(groups = "deployment")
    public void testNotAuthorized() {
        String url = getRestAPIHostPort() + "/pls/companyprofiles/";
        boolean thrown = false;
        CompanyProfileRequest request = new CompanyProfileRequest();
        request.getRecord().put("Email", "someuser@google.com");
        try {
            HttpClientUtils.newRestTemplate().postForObject(url, request, CompanyProfile.class);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            assertTrue(e.getMessage().contains("401"));
            thrown = true;
        }
        assertTrue(thrown);
    }
}
