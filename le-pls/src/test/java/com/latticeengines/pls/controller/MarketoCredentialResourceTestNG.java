package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;

public class MarketoCredentialResourceTestNG extends PlsFunctionalTestNGBaseDeprecated {

    private static final String PLS_MARKETO_CREDENTIAL_URL = "/pls/marketo/credentials/";

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setupUsers();
    }

    @Test(groups = { "functional" })
    public void findMarketoCredentialAsDifferentUsers_assertCorrectBehavior() {
        switchToThirdPartyUser();
        assertGetSimplifiedCredentialsSuccess();
        assertGetFullCredentialsFailed();

        switchToExternalUser();
        assertGetSimplifiedCredentialsSuccess();
        assertGetFullCredentialsSuccess();

        switchToInternalAdmin();
        assertGetSimplifiedCredentialsSuccess();
        assertGetFullCredentialsSuccess();
    }

    private void assertGetSimplifiedCredentialsSuccess() {
        List response = restTemplate.getForObject(
                getRestAPIHostPort() + PLS_MARKETO_CREDENTIAL_URL + "simplified", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 0);
    }

    private void assertGetFullCredentialsSuccess() {
        List response = restTemplate.getForObject(
                getRestAPIHostPort() + PLS_MARKETO_CREDENTIAL_URL, List.class);
        assertNotNull(response);
        assertEquals(response.size(), 0);
    }

    private void assertGetFullCredentialsFailed() {
        boolean exception = false;
        try {
            List response = restTemplate.getForObject(
                    getRestAPIHostPort() + PLS_MARKETO_CREDENTIAL_URL, List.class);
        } catch (Exception e) {
            exception = true;
        }
        assertTrue(exception, "Should have thrown an exception");
    }

}
