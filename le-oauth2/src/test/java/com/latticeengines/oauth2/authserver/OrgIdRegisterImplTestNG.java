package com.latticeengines.oauth2.authserver;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;

public class OrgIdRegisterImplTestNG {

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void registerOrgId() throws Exception {

        String tenantId = "playmakerOrgID";

        HttpServletRequest request = mock(HttpServletRequest.class);
        ServletRequestAttributes attributes = new ServletRequestAttributes(request);

        RequestContextHolder.setRequestAttributes(attributes);
        OrgIdRegisterImpl orgIdRegister = new OrgIdRegisterImpl();
        BatonServiceImpl batonService = new BatonServiceImpl();
        orgIdRegister.setBatonService(batonService);

        orgIdRegister.registerOrgId(tenantId);
        verify(request, times(0)).getParameter("isProduction");

        when(request.getParameter("orgId")).thenReturn("prod123");
        orgIdRegister.registerOrgId(tenantId);
        verify(request, times(1)).getParameter("isProduction");

        // Prod orgId
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        CustomerSpaceInfo spaceInfo = SpaceLifecycleManager.getInfo(customerSpace.getContractId(),
                customerSpace.getTenantId(), customerSpace.getSpaceId());
        Assert.assertEquals(spaceInfo.properties.sfdcOrgId, "prod123");

        // Sandbox orgId
        when(request.getParameter("orgId")).thenReturn("sandbox123");
        when(request.getParameter("isProduction")).thenReturn("false");
        orgIdRegister.registerOrgId(tenantId);
        spaceInfo = SpaceLifecycleManager.getInfo(customerSpace.getContractId(), customerSpace.getTenantId(),
                customerSpace.getSpaceId());
        Assert.assertEquals(spaceInfo.properties.sandboxSfdcOrgId, "sandbox123");
        Assert.assertEquals(spaceInfo.properties.sfdcOrgId, "prod123");

    }
}
