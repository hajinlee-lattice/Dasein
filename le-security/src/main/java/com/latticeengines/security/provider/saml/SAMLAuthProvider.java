package com.latticeengines.security.provider.saml;

import java.util.List;

import org.opensaml.saml2.core.Assertion;
import org.opensaml.saml2.core.impl.ResponseImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.saml.SAMLAuthenticationToken;
import org.springframework.stereotype.Component;

import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;

@Component("samlAuthProvider")
public class SAMLAuthProvider implements AuthenticationProvider {

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {

        try {
            SAMLAuthenticationToken samlAuth = (SAMLAuthenticationToken) authentication;
            ResponseImpl samlResponse = (ResponseImpl) samlAuth.getCredentials().getInboundSAMLMessage();

            List<Assertion> assertions = samlResponse.getAssertions();

            String userId = null;
            for (Assertion assertion : assertions) {
                userId = assertion.getSubject().getNameID().getValue();
                if (userId != null) {
                    break;
                }
            }
            globalAuthenticationService.externallyAuthenticated(userId);

            TicketAuthenticationToken token = new TicketAuthenticationToken( //
                    userId);
            token.setAuthenticated(true);

            return token;
        } catch (Exception e) {
            throw new BadCredentialsException(e.getMessage(), e);
        }

    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authentication.isAssignableFrom(SAMLAuthenticationToken.class);
    }

}
