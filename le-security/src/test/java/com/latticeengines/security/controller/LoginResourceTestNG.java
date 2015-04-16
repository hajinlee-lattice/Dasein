package com.latticeengines.security.controller;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.RightsUtilities;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class LoginResourceTestNG extends SecurityFunctionalTestNGBase {

//    @Test(groups = { "functional", "deployment" })
//    public void login() {
//        Credentials creds = new Credentials();
//        creds.setUsername(adminUsername);
//        creds.setPassword(DigestUtils.sha256Hex(adminPassword));
//
//        LoginDocument loginDoc = restTemplate.postForObject(getRestAPIHostPort() + "/security/login", creds, LoginDocument.class);
//        assertTrue(loginDoc.getResult().getTenants().size() >= 2);
//        assertNotNull(loginDoc.getData());
//    }

    @Test(groups = { "functional", "deployment" })
    public void loginToGlobalAdminTenant() {
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword(DigestUtils.sha256Hex(adminPassword));

        LoginDocument loginDoc = restTemplate.postForObject(getRestAPIHostPort() + "/security/login", creds, LoginDocument.class);
        assertTrue(loginDoc.getResult().getTenants().size() >= 2);
        assertNotNull(loginDoc.getData());

        Tenant tenant = new Tenant();
        tenant.setId(Constants.GLOBAL_ADMIN_TENANT_ID);
        tenant.setName(Constants.GLOBAL_ADMIN_TENANT_NAME);
        addAuthHeader.setAuthValue(loginDoc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));
        UserDocument userDoc = restTemplate.postForObject(getRestAPIHostPort() + "/security/attach", tenant, UserDocument.class);
        List<String> rights = RightsUtilities.translateRights(userDoc.getResult().getUser().getAvailableRights());
        assertTrue(rights.contains(GrantedRight.EDIT_LATTICE_CONFIGURATION.getAuthority()));
    }

//    @Test(groups = { "functional", "deployment" })
//    public void loginBadPassword() {
//        Credentials creds = new Credentials();
//        creds.setUsername(adminUsername);
//        creds.setPassword("badpassword");
//
//        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
//
//        try {
//            restTemplate.postForObject(getRestAPIHostPort() + "/security/login", creds, Session.class);
//        } catch (Exception e) {
//            String code = e.getMessage();
//            assertEquals(code, "401");
//        }
//    }

}
