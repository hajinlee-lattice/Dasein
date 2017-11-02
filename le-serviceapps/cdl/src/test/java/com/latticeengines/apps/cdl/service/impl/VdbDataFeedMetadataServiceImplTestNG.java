package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportVdbTableConfiguration;
import com.latticeengines.domain.exposed.eai.VdbConnectorConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TableUtils;

public class VdbDataFeedMetadataServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(VdbDataFeedMetadataServiceImplTestNG.class);

    private String testVdbMetadata;

    private String errorVdbMetadata;

    private Table testTable;

    private Table resolvedTable;

    @Autowired
    private VdbDataFeedMetadataServiceImpl vdbDataFeedMetadataService;

    @BeforeClass(groups = "functional")
    private void setup() throws IOException {
        // setup test metadata string;
        testVdbMetadata = IOUtils.toString(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("metadata/vdb/testmetadata.json"),
                "UTF-8");
        errorVdbMetadata = IOUtils.toString(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("metadata/vdb/testmetadata_error.json"), "UTF-8");
    }

    @Test(groups = "functional", expectedExceptions = RuntimeException.class)
    public void testGetMetadata() {
        testTable = vdbDataFeedMetadataService.getMetadata(testVdbMetadata);
        Assert.assertNotNull(testTable);
        Assert.assertEquals(testTable.getName(), "FS_Data_For_Dante_Accounts_1000");
        Attribute requiredField = testTable.getAttribute("account");
        Assert.assertNotNull(requiredField);
        Table error = vdbDataFeedMetadataService.getMetadata(errorVdbMetadata);
    }

    @Test(groups = "functional")
    public void testGetCustomerSpace() {
        CustomerSpace customerSpace = vdbDataFeedMetadataService.getCustomerSpace(testVdbMetadata);
        Assert.assertNotNull(customerSpace);
    }

    @Test(groups = "functional")
    public void testGetConnectorConfig() {
        String jobIdentifier = "Vdb_Metadata_Functional_Test";
        String connectorConfig = vdbDataFeedMetadataService.getConnectorConfig(testVdbMetadata, jobIdentifier);
        Assert.assertNotNull(connectorConfig);
        VdbConnectorConfiguration vdbConnectorConfiguration = JsonUtils.deserialize(connectorConfig,
                VdbConnectorConfiguration.class);
        Assert.assertNotNull(vdbConnectorConfiguration);
        Assert.assertNotNull(vdbConnectorConfiguration.getVdbTableConfiguration("FS_Data_For_Dante_Accounts_1000"));
        ImportVdbTableConfiguration importVdbTableConfiguration = vdbConnectorConfiguration
                .getVdbTableConfiguration("FS_Data_For_Dante_Accounts_1000");
        Assert.assertNotNull(importVdbTableConfiguration);
        Assert.assertEquals(importVdbTableConfiguration.getCollectionIdentifier(), jobIdentifier);
    }

    @Test(groups = "functional", dependsOnMethods = "testGetMetadata")
    public void testResolveMetadata() {
        Table schemaTable = SchemaRepository.instance().getSchema(BusinessEntity.Account);

        resolvedTable = vdbDataFeedMetadataService.resolveMetadata(testTable, schemaTable);
        Assert.assertNotNull(resolvedTable);
        Assert.assertNotNull(resolvedTable.getAttribute(InterfaceName.AccountId));
        Assert.assertNotNull(resolvedTable.getAttribute(InterfaceName.Website));
        Assert.assertNotNull(resolvedTable.getAttribute(InterfaceName.CompanyName));
        Assert.assertNotNull(resolvedTable.getAttribute(InterfaceName.SalesforceAccountID));
    }

    @Test(groups = "functional", dependsOnMethods = "testResolveMetadata", expectedExceptions = RuntimeException.class)
    public void testCompareMetadata() {
        Table updateTable1 = TableUtils.clone(resolvedTable, resolvedTable.getName());
        Table updateTable2 = TableUtils.clone(resolvedTable, resolvedTable.getName());
        Table updateTable3 = TableUtils.clone(resolvedTable, resolvedTable.getName());
        Assert.assertTrue(vdbDataFeedMetadataService.compareMetadata(resolvedTable, updateTable1, true));
        updateTable1.getAttribute(InterfaceName.AccountId).setDisplayName("New_Account_ID_Test");
        Assert.assertFalse(vdbDataFeedMetadataService.compareMetadata(resolvedTable, updateTable1, true));
        updateTable2.getAttribute(InterfaceName.CompanyName).setSourceLogicalDataType("long");
        Assert.assertFalse(vdbDataFeedMetadataService.compareMetadata(resolvedTable, updateTable2, false));
        Attribute testAttr = updateTable3.getAttribute("territory");
        String testAttrType = testAttr.getSourceLogicalDataType();
        updateTable3.getAttribute("territory").setSourceLogicalDataType("String");
        updateTable3.getAttribute("territory").setPhysicalDataType("String");
        Assert.assertNotEquals(testAttrType, testAttr.getSourceLogicalDataType());
        Assert.assertTrue(vdbDataFeedMetadataService.compareMetadata(resolvedTable, updateTable3, true));
        Assert.assertEquals(testAttrType, testAttr.getSourceLogicalDataType());
        vdbDataFeedMetadataService.compareMetadata(resolvedTable, updateTable2, true);
    }

}
