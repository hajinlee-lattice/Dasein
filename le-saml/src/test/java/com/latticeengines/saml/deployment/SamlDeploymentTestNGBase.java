package com.latticeengines.saml.deployment;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.saml.service.IdentityProviderService;
import com.latticeengines.saml.testframework.SamlTestNGBase;

public abstract class SamlDeploymentTestNGBase extends SamlTestNGBase {

    @Autowired
    protected IdentityProviderService identityProviderService;

    protected IdentityProvider identityProvider;

    @BeforeClass(groups = "deployment")
    public void setup() throws InterruptedException {
        samlTestBed.setupTenant();

        // Register IdPs
        identityProvider = samlTestBed.constructIdp();
        identityProviderService.create(identityProvider);
        // Sleep to let metadata manager pick up the new IdP
        Thread.sleep(10000);
    }

}
