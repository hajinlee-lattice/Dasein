package com.latticeengines.saml.deployment;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.opensaml.saml2.core.Assertion;
import org.opensaml.saml2.core.Response;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.testframework.security.GlobalAuthTestBed;

public class SamlDeploymentTestNG extends SamlDeploymentTestNGBase {

    @Value("${saml.failure.redirect.address}")
    private String errorPage;

    @Value("${saml.success.redirect.address}")
    private String successPage;

    @Test(groups = "deployment")
    public void testIdPInitiatedAuth() throws UnsupportedEncodingException {
        Response response = samlTestBed.getTestSAMLResponse(identityProvider);
        assertRedirectedToSuccessPage(samlTestBed.sendSamlResponse(response));
    }

    @Test(groups = "deployment")
    public void testIdpInitiatedAuth_IncorrectEntityId() {
        Response response = samlTestBed.getTestSAMLResponse(identityProvider);
        Assertion assertion = response.getAssertions().get(0);
        assertion.getConditions().getAudienceRestrictions().get(0).getAudiences().get(0).setAudienceURI( //
                "invalidEntityId");
        assertRedirectedToErrorPage(samlTestBed.sendSamlResponse(response));
    }

    @Test(groups = "deployment")
    public void testIdpInitiatedAuth_UnknownUser() {
        Response response = samlTestBed.getTestSAMLResponse(identityProvider);
        Assertion assertion = response.getAssertions().get(0);
        assertion.getSubject().getNameID().setValue("unknown@lattice-engines.com");
        assertRedirectedToErrorPage(samlTestBed.sendSamlResponse(response));
    }

    @Test(groups = "deployment")
    public void testIdpInitiatedAuth_IdpNotAssociatedWithTenant() throws InterruptedException {
        GlobalAuthTestBed gatestbed = samlTestBed.getGlobalAuthTestBed();
        gatestbed.setMainTestTenant(gatestbed.getTestTenants().get(1));
        MultiTenantContext.setTenant(gatestbed.getMainTestTenant());

        // Register IdP
        IdentityProvider otherIdentityProvider = samlTestBed.constructIdp();
        identityProviderService.create(otherIdentityProvider);
        // Sleep to let metadata manager pick up the new IdP
        Thread.sleep(10000);

        // Switch back to main tenant
        gatestbed.setMainTestTenant(gatestbed.getTestTenants().get(0));
        MultiTenantContext.setTenant(gatestbed.getMainTestTenant());

        Response response = samlTestBed.getTestSAMLResponse(otherIdentityProvider);

        assertRedirectedToErrorPage(samlTestBed.sendSamlResponse(response));
    }

    private void assertRedirectedToErrorPage(ResponseEntity<Void> httpResponse) {
        List<String> inner = httpResponse.getHeaders().get("Location");
        assertNotNull(inner);
        assertEquals(inner.size(), 1);
        assertTrue(inner.get(0).contains(errorPage));
    }

    private void assertRedirectedToSuccessPage(ResponseEntity<Void> httpResponse) {
        List<String> inner = httpResponse.getHeaders().get("Location");
        assertNotNull(inner);
        assertEquals(inner.size(), 1);
        assertTrue(inner.get(0).contains(successPage));
    }

}
