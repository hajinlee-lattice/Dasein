package com.latticeengines.saml.testframework;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.testframework.exposed.service.GlobalAuthTestBed;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Component
public class SamlDeploymentTestBed extends SamlTestBed {

    @Resource(name = "deploymentTestBed")
    private GlobalAuthDeploymentTestBed globalAuthDeploymentTestBed;

    @Override
    public GlobalAuthTestBed getGlobalAuthTestBed() {
        return globalAuthDeploymentTestBed;
    }

    @Override
    public void registerIdentityProvider(IdentityProvider identityProvider) {
        globalAuthDeploymentTestBed.getRestTemplate().postForObject(String.format("%s/saml/management/identityproviders/%s",
                baseUrl, globalAuthDeploymentTestBed.getMainTestTenant().getId()), identityProvider, Void.class);
    }
}
