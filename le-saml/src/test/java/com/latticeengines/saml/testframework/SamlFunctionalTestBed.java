package com.latticeengines.saml.testframework;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.saml.service.IdentityProviderService;
import com.latticeengines.testframework.exposed.service.GlobalAuthTestBed;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

@Component
public class SamlFunctionalTestBed extends SamlTestBed {
    @Inject
    private GlobalAuthFunctionalTestBed globalAuthFunctionalTestBed;

    @Inject
    private IdentityProviderService identityProviderService;

    @Override
    public GlobalAuthTestBed getGlobalAuthTestBed() {
        return globalAuthFunctionalTestBed;
    }

    @Override
    public void registerIdentityProvider(IdentityProvider identityProvider) {
        identityProviderService.create(identityProvider);
    }
}
