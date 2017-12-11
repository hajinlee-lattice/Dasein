package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
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
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        List<String> crmIds = new ArrayList<>();
        crmIds.add("accountId");
        crmIds.add("testId");
        crmIds.add(InterfaceName.SalesforceSandboxAccountID.name());
        cdlExternalSystem.setCRMIdList(crmIds);
        cdlExternalSystem.setMapIds(InterfaceName.MarketoAccountID.name() + "," + InterfaceName.EloquaAccountID);
        cdlExternalSystem.setErpIds("TestERPId");
        cdlExternalSystemService.createOrUpdateExternalSystem(customerSpace, cdlExternalSystem);

        List<CDLExternalSystem> systems = cdlExternalSystemService.getAllExternalSystem(customerSpace);
        Assert.assertNotNull(systems);
        Assert.assertEquals(systems.size(), 1);

        Assert.assertEquals(systems.get(0).getCRMIdList().size(), 3);
        Assert.assertEquals(systems.get(0).getMAPIdList().size(), 2);
        Assert.assertEquals(systems.get(0).getERPIdList().size(), 1);
        Assert.assertEquals(systems.get(0).getOtherIdList().size(), 0);

        Assert.assertTrue(systems.get(0).getCrmIds().contains(InterfaceName.SalesforceSandboxAccountID.name()));
        Assert.assertTrue(systems.get(0).getMapIds().contains(InterfaceName.MarketoAccountID.name()));
        Assert.assertTrue(systems.get(0).getErpIds().contains("TestERPId"));
    }

}
