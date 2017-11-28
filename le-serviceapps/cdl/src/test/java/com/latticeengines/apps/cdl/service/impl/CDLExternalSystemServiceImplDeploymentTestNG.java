package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
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
        cdlExternalSystemProxy.createCDLExternalSystem(customerSpace, "crm", InterfaceName.SalesforceAccountID);
        cdlExternalSystemProxy.createCDLExternalSystem(customerSpace, "MAP", InterfaceName.MarketoAccountID);
        List<CDLExternalSystem> systems = cdlExternalSystemProxy.getCDLExternalSystems(customerSpace);
        Assert.assertNotNull(systems);
        Assert.assertEquals(systems.size(), 2);
        boolean containsSFDCProd = false;
        boolean containsMKTO = false;
        for (int i = 0; i < 2; i++) {
            if (systems.get(i).getCRM() == CDLExternalSystem.CRMType.SFDC_Production) {
                containsSFDCProd = true;
            } else if (systems.get(i).getMAP() == CDLExternalSystem.MAPType.Marketo) {
                containsMKTO = true;
            }
        }
        Assert.assertTrue(containsSFDCProd);
        Assert.assertTrue(containsMKTO);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateAndGet")
    public void testCreateAndGet2() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        cdlExternalSystemProxy.createCDLExternalSystem(customerSpace, InterfaceName.SalesforceSandboxAccountID, InterfaceName
                .EloquaAccountID, null);
        List<CDLExternalSystem> systems = cdlExternalSystemProxy.getCDLExternalSystems(customerSpace);
        Assert.assertNotNull(systems);
        Assert.assertEquals(systems.size(), 3);
        Assert.assertEquals(systems.get(2).getCRM(), CDLExternalSystem.CRMType.SFDC_Sandbox);
        Assert.assertEquals(systems.get(2).getMAP(), CDLExternalSystem.MAPType.Eloqua);
        Assert.assertNull(systems.get(2).getERP());

    }
}
