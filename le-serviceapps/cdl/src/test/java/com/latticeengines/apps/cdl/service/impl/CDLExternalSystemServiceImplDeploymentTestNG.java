package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;

public class CDLExternalSystemServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Autowired
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        super.setupTestEnvironment();
    }

    @Test(groups = "deployment")
    public void testCreateAndGet() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        List<String> crmIds = new ArrayList<>();
        crmIds.add("accountId");
        crmIds.add("testId");
        crmIds.add(InterfaceName.SalesforceSandboxAccountID.name());
        cdlExternalSystem.setCRMIdList(crmIds);
        cdlExternalSystem.setMapIds(InterfaceName.MarketoAccountID.name() + "," + InterfaceName.EloquaAccountID);
        cdlExternalSystem.setErpIds("TestERPId");
        cdlExternalSystemProxy.createOrUpdateCDLExternalSystem(customerSpace, cdlExternalSystem);

        CDLExternalSystem system = cdlExternalSystemProxy.getCDLExternalSystem(customerSpace);
        Assert.assertNotNull(system);

        Assert.assertEquals(system.getCRMIdList().size(), 3);
        Assert.assertEquals(system.getMAPIdList().size(), 2);
        Assert.assertEquals(system.getERPIdList().size(), 1);
        Assert.assertEquals(system.getOtherIdList().size(), 0);

        Assert.assertTrue(system.getCrmIds().contains(InterfaceName.SalesforceSandboxAccountID.name()));
        Assert.assertTrue(system.getMapIds().contains(InterfaceName.MarketoAccountID.name()));
        Assert.assertTrue(system.getErpIds().contains("TestERPId"));
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateAndGet")
    public void testUpdate() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        cdlExternalSystemProxy.createOrUpdateCDLExternalSystem(customerSpace, cdlExternalSystem);

        CDLExternalSystem system = cdlExternalSystemProxy.getCDLExternalSystem(customerSpace);
        Assert.assertNotNull(system);

        Assert.assertEquals(system.getCRMIdList().size(), 0);
        Assert.assertEquals(system.getMAPIdList().size(), 0);
        Assert.assertEquals(system.getERPIdList().size(), 0);
        Assert.assertEquals(system.getOtherIdList().size(), 0);
    }

}
