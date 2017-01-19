package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.pls.CompanyProfileAttributeFlags;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class AttributeResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    private CompanyProfileAttributeFlags flags;

    @BeforeClass(groups = "functional")
    public void setup() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @Test(groups = "deployment")
    public void testSaveFlags() {
        String url = getRestAPIHostPort() + "/pls/attributes/flags/TestFlag/CompanyProfile";
        flags = new CompanyProfileAttributeFlags();
        flags.setHidden(true);
        flags.setHighlighted(false);
        restTemplate.postForObject(url, flags, Void.class);
    }

    @Test(groups = "deployment", dependsOnMethods = "testSaveFlags")
    public void testRetrieveFlags() {
        String url = getRestAPIHostPort() + "/pls/attributes/flags/TestFlag/CompanyProfile";
        CompanyProfileAttributeFlags retrieved = restTemplate.getForObject(url, CompanyProfileAttributeFlags.class);
        assertNotNull(retrieved);
        assertEquals(retrieved, flags);
    }
}
