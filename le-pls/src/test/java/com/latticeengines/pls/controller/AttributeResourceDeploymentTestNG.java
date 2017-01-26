package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.pls.CompanyProfileAttributeFlags;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class AttributeResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    private CompanyProfileAttributeFlags flags;
    private String attributeName;

    @BeforeClass(groups = "deployment")
    public void setup() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @Test(groups = "deployment")
    public void testSaveFlags() {
        List raw = restTemplate.getForObject(getRestAPIHostPort() + "/pls/latticeinsights/insights", List.class);
        List<LeadEnrichmentAttribute> attributes = JsonUtils.convertList(raw, LeadEnrichmentAttribute.class);
        attributeName = attributes.get(0).getFieldName();

        String url = getRestAPIHostPort() + String.format("/pls/attributes/flags/%s/CompanyProfile", attributeName);
        flags = new CompanyProfileAttributeFlags(true, false);
        restTemplate.postForObject(url, flags, Void.class);
    }

    @Test(groups = "deployment", dependsOnMethods = "testSaveFlags")
    public void testRetrieveFlags() {
        String url = getRestAPIHostPort() + String.format("/pls/attributes/flags/%s/CompanyProfile", attributeName);
        CompanyProfileAttributeFlags retrieved = restTemplate.getForObject(url, CompanyProfileAttributeFlags.class);
        assertNotNull(retrieved);
        assertEquals(retrieved, flags);
    }
}
