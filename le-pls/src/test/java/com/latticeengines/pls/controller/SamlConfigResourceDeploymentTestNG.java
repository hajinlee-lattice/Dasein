package com.latticeengines.pls.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.saml.ServiceProviderURIInfo;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class SamlConfigResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SamlConfigResourceDeploymentTestNG.class);

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        log.info(String.format("Using tenant: %s for test", mainTestTenant.getId()));
    }

    @Test(groups = { "deployment" })
    public void testSPUriConfig() {
        String url = getRestAPIHostPort() + "/pls/saml-config/sp-uri-info";

        ServiceProviderURIInfo resp = restTemplate.getForObject(url, ServiceProviderURIInfo.class);

        Assert.assertNotNull(resp);
        Assert.assertNotNull(resp.getAssertionConsumerServiceURL());
        Assert.assertNotNull(resp.getServiceProviderEntityId());
        Assert.assertNotNull(resp.getServiceProviderMetadataURL());

        Assert.assertTrue(resp.getAssertionConsumerServiceURL().contains(mainTestTenant.getId()));
        Assert.assertTrue(resp.getServiceProviderEntityId().contains(mainTestTenant.getId()));
        // make sure that SP metadata Url does not have tenant id in its path
        Assert.assertFalse(resp.getServiceProviderMetadataURL().contains(mainTestTenant.getId()));
    }
}
