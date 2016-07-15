package com.latticeengines.saml.testframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.testframework.security.GlobalAuthTestBed;
import com.latticeengines.testframework.security.impl.GlobalAuthDeploymentTestBed;

@Component
public class SamlDeploymentTestBed extends SamlTestBed {
    @Autowired
    @Qualifier(value = "deploymentTestBed")
    private GlobalAuthDeploymentTestBed globalAuthDeploymentTestBed;

    @Override
    public GlobalAuthTestBed getGlobalAuthTestBed() {
        return globalAuthDeploymentTestBed;
    }

    @Override
    public void registerIdentityProvider(IdentityProvider identityProvider) {
        globalAuthDeploymentTestBed.getRestTemplate().postForObject(
                String.format("%s/management/identityproviders", baseUrl), identityProvider, Void.class);
    }
}
