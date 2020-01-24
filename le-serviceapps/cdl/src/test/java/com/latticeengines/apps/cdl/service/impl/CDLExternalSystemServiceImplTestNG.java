package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
public class CDLExternalSystemServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
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
        crmIds.add("account_Id");
        crmIds.add("test_ID");
        crmIds.add(InterfaceName.SalesforceSandboxAccountID.name());
        cdlExternalSystem.setCRMIdList(crmIds);
        cdlExternalSystem.setEntity(BusinessEntity.Account);
        cdlExternalSystem.setMapIds(InterfaceName.MarketoAccountID.name() + "," + InterfaceName.EloquaAccountID);
        cdlExternalSystem.setErpIds("Test_ERP_Id");

        cdlExternalSystemService.createOrUpdateExternalSystem(customerSpace, cdlExternalSystem, BusinessEntity.Account);

        List<CDLExternalSystem> systems = cdlExternalSystemService.getAllExternalSystem(customerSpace);
        Assert.assertNotNull(systems);
        Assert.assertEquals(systems.size(), 1);

        Assert.assertEquals(systems.get(0).getCRMIdList().size(), 3);
        Assert.assertEquals(systems.get(0).getMAPIdList().size(), 2);
        Assert.assertEquals(systems.get(0).getERPIdList().size(), 1);
        Assert.assertEquals(systems.get(0).getOtherIdList().size(), 0);
        Assert.assertEquals(systems.get(0).getEntity(), BusinessEntity.Account);

        Assert.assertTrue(systems.get(0).getCrmIds().contains(InterfaceName.SalesforceSandboxAccountID.name()));
        Assert.assertTrue(systems.get(0).getMapIds().contains(InterfaceName.MarketoAccountID.name()));
        Assert.assertTrue(systems.get(0).getErpIds().contains("Test_ERP_Id"));

        List<Pair<String, String>> idMappings = new ArrayList<>();
        idMappings.add(Pair.of("account_Id", "account_Id"));
        idMappings.add(Pair.of("test_ID", "test"));
        idMappings.add(Pair.of(InterfaceName.SalesforceSandboxAccountID.name(), "SalesforceSandboxAccountID"));
        idMappings.add(Pair.of(InterfaceName.MarketoAccountID.name(), "MarketoAccountID"));
        idMappings.add(Pair.of(InterfaceName.EloquaAccountID.name(), "EloquaAccountID"));
        idMappings.add(Pair.of("Test_ERP_Id", "Test ERP Id"));

        cdlExternalSystem.addIdMapping(idMappings);
        cdlExternalSystemService.createOrUpdateExternalSystem(customerSpace, cdlExternalSystem, BusinessEntity.Account);

        cdlExternalSystem = cdlExternalSystemService.getExternalSystem(customerSpace, BusinessEntity.Account);

        Assert.assertEquals(cdlExternalSystem.getDisplayNameById("Test_ERP_Id"), "Test ERP Id");
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateAndGet")
    public void testUpdate() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        CDLExternalSystem cdlExternalSystem = cdlExternalSystemService.getExternalSystem(customerSpace, BusinessEntity.Account);

        List<String> crmIds = new ArrayList<>(cdlExternalSystem.getCRMIdList());
        crmIds.add("Strange_External_ID");
        cdlExternalSystem.setCRMIdList(crmIds);
        cdlExternalSystem.addIdMapping(Arrays.asList(Pair.of("Strange_External_ID", "Strange External")));
        cdlExternalSystemService.createOrUpdateExternalSystem(customerSpace, cdlExternalSystem, BusinessEntity.Account);

        cdlExternalSystem = cdlExternalSystemService.getExternalSystem(customerSpace, BusinessEntity.Account);

        Assert.assertEquals(cdlExternalSystem.getDisplayNameById("Test_ERP_Id"), "Test ERP Id");
        Assert.assertEquals(cdlExternalSystem.getDisplayNameById("Strange_External_ID"), "Strange External");

        Assert.assertEquals(cdlExternalSystem.getCRMIdList().size(), 4);
    }

    @Test(groups = "functional", enabled = false)
    public void testCreateAndGetForContact() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        List<String> crmIds = new ArrayList<>();
        crmIds.add("account_Id");
        crmIds.add("test_ID");
        crmIds.add(InterfaceName.SalesforceSandboxAccountID.name());
        cdlExternalSystem.setCRMIdList(crmIds);
        cdlExternalSystem.setEntity(BusinessEntity.Contact);
        cdlExternalSystem.setMapIds(InterfaceName.MarketoAccountID.name() + "," + InterfaceName.EloquaAccountID);
        cdlExternalSystem.setErpIds("Test_ERP_Id");

        cdlExternalSystemService.createOrUpdateExternalSystem(customerSpace, cdlExternalSystem, BusinessEntity.Contact);

        List<CDLExternalSystem> systems = cdlExternalSystemService.getAllExternalSystem(customerSpace);
        Assert.assertNotNull(systems);
        Assert.assertEquals(systems.size(), 1);

        Assert.assertEquals(systems.get(0).getCRMIdList().size(), 3);
        Assert.assertEquals(systems.get(0).getMAPIdList().size(), 2);
        Assert.assertEquals(systems.get(0).getERPIdList().size(), 1);
        Assert.assertEquals(systems.get(0).getOtherIdList().size(), 0);
        Assert.assertEquals(systems.get(0).getEntity(), BusinessEntity.Contact);

        Assert.assertTrue(systems.get(0).getCrmIds().contains(InterfaceName.SalesforceSandboxAccountID.name()));
        Assert.assertTrue(systems.get(0).getMapIds().contains(InterfaceName.MarketoAccountID.name()));
        Assert.assertTrue(systems.get(0).getErpIds().contains("Test_ERP_Id"));

        List<Pair<String, String>> idMappings = new ArrayList<>();
        idMappings.add(Pair.of("account_Id", "account_Id"));
        idMappings.add(Pair.of("test_ID", "test"));
        idMappings.add(Pair.of(InterfaceName.SalesforceSandboxAccountID.name(), "SalesforceSandboxAccountID"));
        idMappings.add(Pair.of(InterfaceName.MarketoAccountID.name(), "MarketoAccountID"));
        idMappings.add(Pair.of(InterfaceName.EloquaAccountID.name(), "EloquaAccountID"));
        idMappings.add(Pair.of("Test_ERP_Id", "Test ERP Id"));

        cdlExternalSystem.addIdMapping(idMappings);
        cdlExternalSystemService.createOrUpdateExternalSystem(customerSpace, cdlExternalSystem, BusinessEntity.Contact);

        cdlExternalSystem = cdlExternalSystemService.getExternalSystem(customerSpace, BusinessEntity.Contact);

        Assert.assertEquals(cdlExternalSystem.getDisplayNameById("Test_ERP_Id"), "Test ERP Id");
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateAndGetForContact", enabled = false)
    public void testUpdateContact() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        CDLExternalSystem cdlExternalSystem = cdlExternalSystemService.getExternalSystem(customerSpace, BusinessEntity.Contact);

        List<String> crmIds = new ArrayList<>(cdlExternalSystem.getCRMIdList());
        crmIds.add("Strange_External_ID");
        cdlExternalSystem.setCRMIdList(crmIds);
        cdlExternalSystem.addIdMapping(Arrays.asList(Pair.of("Strange_External_ID", "Strange External")));
        cdlExternalSystemService.createOrUpdateExternalSystem(customerSpace, cdlExternalSystem, BusinessEntity.Contact);

        cdlExternalSystem = cdlExternalSystemService.getExternalSystem(customerSpace, BusinessEntity.Contact);

        Assert.assertEquals(cdlExternalSystem.getDisplayNameById("Test_ERP_Id"), "Test ERP Id");
        Assert.assertEquals(cdlExternalSystem.getDisplayNameById("Strange_External_ID"), "Strange External");

        Assert.assertEquals(cdlExternalSystem.getCRMIdList().size(), 4);
    }

}
