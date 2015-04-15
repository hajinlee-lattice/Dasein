package com.latticeengines.security.controller;

import static org.testng.Assert.assertEquals;
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
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.RightsUtilities;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class GlobalAdminTenantResourceTestNG extends SecurityFunctionalTestNGBase {

    @Test(groups = { "functional", "deployment" })
    public void get() {
        Tenant tenant = restTemplate.getForObject(getRestAPIHostPort() + "/security/globaladmintenant", Tenant.class);
        assertEquals(tenant.getId(), Constants.GLOBAL_ADMIN_TENANT_ID);
        assertEquals(tenant.getName(), Constants.GLOBAL_ADMIN_TENANT_NAME);
    }
}
