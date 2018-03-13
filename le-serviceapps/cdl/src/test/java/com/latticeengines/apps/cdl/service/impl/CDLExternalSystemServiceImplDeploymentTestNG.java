package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class CDLExternalSystemServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Autowired
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Inject
    private SystemMetadataStore systemMetadataStore;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @BeforeClass(groups = "deployment")
    public void setup() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        super.setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId());
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
    public void testLookupIdAttrGroup() {
        List<ColumnMetadata> cms = systemMetadataStore.getMetadata(BusinessEntity.Account, DataCollection.Version.Blue)
                .filter(cm -> cm.isEnabledFor(ColumnSelection.Predefined.LookupId)).collectList().block();
        Assert.assertTrue(CollectionUtils.isNotEmpty(cms));
    }

    private void prepareTable() {
        Table table = new Table();
        table.setName("TestExternalSystemTable");
        table.setDisplayName("TestExternalSystemTable");
        Attribute attr1 = new Attribute("accountId");
        attr1.setDisplayName("TestAccountID");
        attr1.setPhysicalDataType("String");
        Attribute attr2 = new Attribute("testId");
        attr2.setDisplayName("TestDummyID");
        attr2.setPhysicalDataType("String");
        Attribute attr3 = new Attribute(InterfaceName.MarketoAccountID.name());
        attr3.setDisplayName("TestMarketoID");
        attr3.setPhysicalDataType("String");
        table.setAttributes(Arrays.asList(attr1, attr2, attr3));
        metadataProxy.createTable(mainCustomerSpace, table.getName(), table);
        dataCollectionProxy.upsertTable(mainCustomerSpace, table.getName(), TableRoleInCollection.BucketedAccount, DataCollection.Version.Blue);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateAndGet")
    public void testGetMapping() {
        prepareTable();
        List<CDLExternalSystemMapping> crmList = cdlExternalSystemProxy.getExternalSystemByType(mainCustomerSpace,
                CDLExternalSystemType.CRM);
        Assert.assertEquals(crmList.size(), 2);
        for (CDLExternalSystemMapping cesm : crmList) {
            if (cesm.getFieldName().equals("accountId")) {
                Assert.assertEquals(cesm.getDisplayName(), "TestAccountID");
            } else if (cesm.getFieldName().equals("testId")) {
                Assert.assertEquals(cesm.getDisplayName(), "TestDummyID");
            } else {
                Assert.assertTrue("External system not the same as input", false);
            }
        }
        Map<String, List<CDLExternalSystemMapping>> mapping =
                cdlExternalSystemProxy.getExternalSystemMap(mainCustomerSpace);
        Assert.assertEquals(mapping.size(), 2);
        List<CDLExternalSystemMapping> mapList = mapping.get(CDLExternalSystemType.MAP.name());
        Assert.assertEquals(mapList.size(), 1);
        Assert.assertEquals(mapList.get(0).getFieldName(), InterfaceName.MarketoAccountID.name());
        Assert.assertEquals(mapList.get(0).getDisplayName(), "TestMarketoID");
    }


    @Test(groups = "deployment", dependsOnMethods = "testGetMapping")
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
