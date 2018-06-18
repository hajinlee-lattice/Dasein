package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.mds.ExternalSystemMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class CDLExternalSystemServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @Inject
    private SystemMetadataStore systemMetadataStore;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private ExternalSystemMetadataStore externalSystemMetadataStore;

    @BeforeClass(groups = "deployment")
    public void setup() {
        super.setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId());
        List<ColumnMetadata> cms = systemMetadataStore.getMetadata(BusinessEntity.Account, DataCollection.Version.Blue)
                .collectList().block();
        Assert.assertTrue(CollectionUtils.isNotEmpty(cms));
    }

    @Test(groups = "deployment")
    public void testCreateAndGet() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        List<String> crmIds = new ArrayList<>();
        crmIds.add("TechIndicator_MarketGID");
        crmIds.add("TechIndicator_MGID");
        crmIds.add("TechIndicator_Squid");
        cdlExternalSystem.setCRMIdList(crmIds);
        cdlExternalSystem.setMapIds("TechIndicator_CAVSAMAid" + "," + "TechIndicator_Candid");
        cdlExternalSystem.setErpIds("TechIndicator_SysAid");
        cdlExternalSystemProxy.createOrUpdateCDLExternalSystem(customerSpace, cdlExternalSystem);

        CDLExternalSystem system = cdlExternalSystemProxy.getCDLExternalSystem(customerSpace);
        Assert.assertNotNull(system);

        Assert.assertEquals(system.getCRMIdList().size(), 3);
        Assert.assertEquals(system.getMAPIdList().size(), 2);
        Assert.assertEquals(system.getERPIdList().size(), 1);
        Assert.assertEquals(system.getOtherIdList().size(), 0);

        Assert.assertTrue(system.getCrmIds().contains("TechIndicator_MarketGID"));
        Assert.assertTrue(system.getMapIds().contains("TechIndicator_CAVSAMAid"));
        Assert.assertTrue(system.getErpIds().contains("TechIndicator_SysAid"));
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateAndGet", enabled = true)
    public void testLookupIdAttrGroup() {
        String tenantId = CustomerSpace.parse(mainCustomerSpace).getTenantId();
        List<ColumnMetadata> cms = externalSystemMetadataStore.getMetadata(tenantId, BusinessEntity.Account).collectList().block();
        Assert.assertTrue(CollectionUtils.isNotEmpty(cms));

        cms = systemMetadataStore.getMetadata(BusinessEntity.Account, DataCollection.Version.Blue)
                .filter(cm -> cm.isEnabledFor(ColumnSelection.Predefined.LookupId)).collectList().block();
        Assert.assertTrue(CollectionUtils.isNotEmpty(cms));
    }


    @Test(groups = "deployment", dependsOnMethods = "testCreateAndGet")
    public void testGetMapping() {
        String tenantId = CustomerSpace.parse(mainCustomerSpace).getTenantId();
        List<ColumnMetadata> cms = externalSystemMetadataStore.getMetadata(tenantId, BusinessEntity.Account).collectList().block();
        Assert.assertTrue(CollectionUtils.isNotEmpty(cms));
        List<CDLExternalSystemMapping> crmList = cdlExternalSystemProxy.getExternalSystemByType(mainCustomerSpace,
                CDLExternalSystemType.CRM);
        Assert.assertEquals(crmList.size(), 3);

        Map<String, List<CDLExternalSystemMapping>> mapping =
                cdlExternalSystemProxy.getExternalSystemMap(mainCustomerSpace);
        Assert.assertEquals(mapping.size(), 3);
        List<CDLExternalSystemMapping> mapList = mapping.get(CDLExternalSystemType.ERP.name());
        Assert.assertEquals(mapList.size(), 1);
        Assert.assertEquals(mapList.get(0).getFieldName(), "TechIndicator_SysAid");
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
