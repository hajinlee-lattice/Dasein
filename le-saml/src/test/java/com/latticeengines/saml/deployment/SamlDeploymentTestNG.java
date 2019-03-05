package com.latticeengines.saml.deployment;

import org.opensaml.saml2.core.Assertion;
import org.opensaml.saml2.core.Response;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.testframework.exposed.service.GlobalAuthTestBed;

public class SamlDeploymentTestNG extends SamlDeploymentTestNGBase {

    @Test(groups = "deployment")
    public void testIdPInitiatedAuth() {
        Response response = samlDeploymentTestBed.getTestSAMLResponse(identityProvider);
        assertRedirectedToSuccessPage(samlDeploymentTestBed.sendSamlResponse(response));
    }

    @Test(groups = "deployment")
    public void testIdPInitiatedAuth_ResponseNotSigned() {
        Response response = samlDeploymentTestBed.getTestSAMLResponse(identityProvider);
        assertRedirectedToErrorPage(samlDeploymentTestBed.sendSamlResponse(response, false));
    }

    @Test(groups = "deployment")
    public void testIdpInitiatedAuth_IncorrectEntityId() {
        Response response = samlDeploymentTestBed.getTestSAMLResponse(identityProvider);
        Assertion assertion = response.getAssertions().get(0);
        assertion.getConditions().getAudienceRestrictions().get(0).getAudiences().get(0).setAudienceURI( //
                "invalidEntityId");
        assertRedirectedToErrorPage(samlDeploymentTestBed.sendSamlResponse(response));
    }

    @Test(groups = "deployment")
    public void testIdpInitiatedAuth_UnknownUser() {
        Response response = samlDeploymentTestBed.getTestSAMLResponse(identityProvider);
        Assertion assertion = response.getAssertions().get(0);
        assertion.getSubject().getNameID().setValue("unknown@lattice-engines.com");
        assertRedirectedToErrorPage(samlDeploymentTestBed.sendSamlResponse(response));
    }

    /**
     * This tests the scenario where we create an identity provider associated
     * with tenant A, but attempt to login with it using tenant B.
     */
    @Test(groups = "deployment")
    public void testIdpInitiatedAuth_IdpNotAssociatedWithTenant() throws InterruptedException {
        GlobalAuthTestBed gatestbed = samlDeploymentTestBed.getGlobalAuthTestBed();

        // Switch to secondary tenant
        gatestbed.setMainTestTenant(gatestbed.getTestTenants().get(1));
        gatestbed.switchToSuperAdmin(gatestbed.getMainTestTenant());
        MultiTenantContext.setTenant(gatestbed.getMainTestTenant());

        // Register IdP
        IdentityProvider otherIdentityProvider = samlDeploymentTestBed.constructIdp();
        samlDeploymentTestBed.registerIdentityProvider(otherIdentityProvider);
        // Sleep to let metadata manager pick up the new IdP
        Thread.sleep(10000);

        // Switch back to main tenant
        gatestbed.setMainTestTenant(gatestbed.getTestTenants().get(0));
        gatestbed.switchToSuperAdmin(gatestbed.getMainTestTenant());
        MultiTenantContext.setTenant(gatestbed.getMainTestTenant());

        // Try to login
        Response response = samlDeploymentTestBed.getTestSAMLResponse(otherIdentityProvider);
        assertRedirectedToErrorPage(samlDeploymentTestBed.sendSamlResponse(response));
    }

    private void assertRedirectedToErrorPage(LoginValidationResponse response) {
        Assert.assertNotNull(response);
        Assert.assertFalse(response.isValidated());
        Assert.assertNull(response.getUserId());
        Assert.assertNotNull(response.getAuthenticationException());
    }

    private void assertRedirectedToSuccessPage(LoginValidationResponse response) {
        Assert.assertNotNull(response);
        Assert.assertTrue(response.isValidated());
        Assert.assertNotNull(response.getUserId());
        Assert.assertNull(response.getAuthenticationException());
    }
}
