package com.latticeengines.saml.deployment;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.UnsupportedEncodingException;

import org.opensaml.saml2.core.Assertion;
import org.opensaml.saml2.core.Response;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class SamlDeploymentTestNG extends SamlDeploymentTestNGBase {

    @Test(groups = "deployment")
    public void testIdPInitiatedAuth() throws UnsupportedEncodingException {
        Response response = getTestSAMLResponse(identityProvider);
        sendSamlResponse(response);
    }

    @Test(groups = "deployment")
    public void testIdpInitiatedAuth_IncorrectEntityId() {
        Response response = getTestSAMLResponse(identityProvider);
        Assertion assertion = response.getAssertions().get(0);
        assertion.getConditions().getAudienceRestrictions().get(0).getAudiences().get(0).setAudienceURI( //
                "invalidEntityId");
        boolean thrown = false;
        try {
            sendSamlResponse(response);
        } catch (HttpClientErrorException e) {
            assertEquals(e.getStatusCode(), HttpStatus.UNAUTHORIZED);
            thrown = true;
        }

        assertTrue(thrown);
    }

    @Test(groups = "deployment")
    public void testIdpInitiatedAuth_UnknownUser() {
        Response response = getTestSAMLResponse(identityProvider);
        Assertion assertion = response.getAssertions().get(0);
        assertion.getSubject().getNameID().setValue("unknown@lattice-engines.com");
        boolean thrown = false;
        try {
            sendSamlResponse(response);
        } catch (HttpClientErrorException e) {
            assertEquals(e.getStatusCode(), HttpStatus.UNAUTHORIZED);
            thrown = true;
        }

        assertTrue(thrown);
    }

    @Test(groups = "deployment")
    public void testIdpInitiatedAuth_IdpNotAssociatedWithTenant() throws InterruptedException {
        globalAuthFunctionalTestBed.setMainTestTenant(globalAuthFunctionalTestBed.getTestTenants().get(1));
        MultiTenantContext.setTenant(globalAuthFunctionalTestBed.getMainTestTenant());

        // Register IdP
        IdentityProvider otherIdentityProvider = constructIdp();
        identityProviderService.create(otherIdentityProvider);
        // Sleep to let metadata manager pick up the new IdP
        Thread.sleep(10000);

        // Switch back to main tenant
        globalAuthFunctionalTestBed.setMainTestTenant(globalAuthFunctionalTestBed.getTestTenants().get(0));
        MultiTenantContext.setTenant(globalAuthFunctionalTestBed.getMainTestTenant());

        Response response = getTestSAMLResponse(otherIdentityProvider);

        boolean thrown = false;
        try {
            sendSamlResponse(response);
        } catch (HttpClientErrorException e) {
            assertEquals(e.getStatusCode(), HttpStatus.UNAUTHORIZED);
            thrown = true;
        }

        assertTrue(thrown);
    }
}
