package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.impl.Constants;

public class AdminResourceTestNG extends PlsFunctionalTestNGBase {

    private Tenant tenant;
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    
    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() {
        tenantEntityMgr.deleteAll();
        tenant = new Tenant();
        tenant.setId("T1");
        tenant.setName("T1");
    }
    
    @Test(groups = { "functional", "deployment" })
    public void addTenantWithProperMagicAuthenticationHeader() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Boolean result = restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class, new HashMap<>());
        assertTrue(result);
        
        Tenant t = tenantEntityMgr.findByTenantId("T1");
        assertNotNull(t);
    }

    @Test(groups = { "functional", "deployment" })
    public void addTenantWithoutProperMagicAuthenticationHeader() {
        addMagicAuthHeader.setAuthValue("xyz");
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        boolean exception = false;
        try {
            restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class, new HashMap<>());
        } catch (Exception e) {
            exception = true;
            String code = e.getMessage();
            assertEquals(code, "401");
        }
        assertTrue(exception);
    }
}
