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
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

public class OrgIdRegisterImplTestNG {

    private BatonServiceImpl batonService;
    private String tenantId = "playmakerOrgID";

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
        batonService = new BatonServiceImpl();
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(new CustomerSpaceProperties(), "");
        batonService.createTenant(customerSpace.getContractId(), tenantId, customerSpace.getSpaceId(), spaceInfo);
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void registerOrgId() throws Exception {

        HttpServletRequest request = mock(HttpServletRequest.class);
        ServletRequestAttributes attributes = new ServletRequestAttributes(request);

        RequestContextHolder.setRequestAttributes(attributes);
        OrgIdRegisterImpl orgIdRegister = new OrgIdRegisterImpl();

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
