package com.latticeengines.saml.deployment;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.saml.testframework.SamlTestBed;
import com.latticeengines.saml.testframework.SamlTestNGBase;

public abstract class SamlDeploymentTestNGBase extends SamlTestNGBase {

    @Inject
    protected SamlTestBed samlDeploymentTestBed;

    protected IdentityProvider identityProvider;

    @BeforeClass(groups = "deployment")
    public void setup() throws InterruptedException {
        samlDeploymentTestBed.setupTenant();

        // Register IdPs
        identityProvider = samlDeploymentTestBed.constructIdp();
        samlDeploymentTestBed.registerIdentityProvider(identityProvider);

        // Sleep to let metadata manager pick up the new IdP
        Thread.sleep(10000);
    }

}
