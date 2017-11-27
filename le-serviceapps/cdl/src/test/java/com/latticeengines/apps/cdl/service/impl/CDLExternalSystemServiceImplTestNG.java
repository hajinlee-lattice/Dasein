package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class CDLExternalSystemServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Autowired
    private CDLExternalSystemService cdlExternalSystemService;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setupTestEnvironmentWithDummySegment();
    }

    @Test(groups = "functional")
    public void testCreateAndGet() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        cdlExternalSystemService.createExternalSystem(customerSpace, "crm", InterfaceName.SFDC_Production_ID);
        cdlExternalSystemService.createExternalSystem(customerSpace, "MAP", InterfaceName.MKTO_ID);
        List<CDLExternalSystem> systems = cdlExternalSystemService.getAllExternalSystem(customerSpace);
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

    @Test(groups = "functional", dependsOnMethods = "testCreateAndGet")
    public void testCreateAndGet2() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        cdlExternalSystemService.createExternalSystem(customerSpace, InterfaceName.SFDC_Sandbox_ID, InterfaceName
                .ELOQUA_ID, null);
        List<CDLExternalSystem> systems = cdlExternalSystemService.getAllExternalSystem(customerSpace);
        Assert.assertNotNull(systems);
        Assert.assertEquals(systems.size(), 3);
        Assert.assertEquals(systems.get(2).getCRM(), CDLExternalSystem.CRMType.SFDC_Sandbox);
        Assert.assertEquals(systems.get(2).getMAP(), CDLExternalSystem.MAPType.Eloqua);
        Assert.assertNull(systems.get(2).getERP());

    }
}
