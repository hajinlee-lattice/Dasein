package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.util.Arrays;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.VdbImportConfig;
import com.latticeengines.domain.exposed.eai.ImportVdbTableConfiguration;
import com.latticeengines.domain.exposed.eai.VdbConnectorConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TableUtils;
public class VdbDataFeedMetadataServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(VdbDataFeedMetadataServiceImplTestNG.class);

    private VdbImportConfig testVdbMetadata;

    private VdbImportConfig errorVdbMetadata;

    private VdbImportConfig vdbMetadata1;

    private VdbImportConfig vdbMetadata2;

    private Table testTable;

    private Table resolvedTable;

    @Inject
    private VdbDataFeedMetadataServiceImpl vdbDataFeedMetadataService;

    @Inject
    private DataFeedTaskManagerServiceImpl dataFeedTaskManagerService;

    @Inject
    private CDLExternalSystemService cdlExternalSystemService;

    @BeforeClass(groups = "functional")
    private void setup() throws IOException {
        // setup test metadata string;
        super.setupTestEnvironment();
        testVdbMetadata = new VdbImportConfig();
        errorVdbMetadata = new VdbImportConfig();
        testVdbMetadata
                .setVdbLoadTableConfig(JsonUtils.deserialize(
                        IOUtils.toString(Thread.currentThread().getContextClassLoader()
                                .getResourceAsStream("metadata/vdb/testmetadata.json"), "UTF-8"),
                        VdbLoadTableConfig.class));
        errorVdbMetadata.setVdbLoadTableConfig(JsonUtils.deserialize(
                IOUtils.toString(Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("metadata/vdb/testmetadata_error.json"), "UTF-8"),
                VdbLoadTableConfig.class));
        vdbMetadata1 = new VdbImportConfig();
        vdbMetadata2 = new VdbImportConfig();
        vdbMetadata1
                .setVdbLoadTableConfig(
                        JsonUtils.deserialize(
                                IOUtils.toString(Thread.currentThread().getContextClassLoader()
                                        .getResourceAsStream("metadata/vdb/test1.json"), "UTF-8"),
                                VdbLoadTableConfig.class));
        vdbMetadata2
                .setVdbLoadTableConfig(
                        JsonUtils.deserialize(
                                IOUtils.toString(Thread.currentThread().getContextClassLoader()
                                        .getResourceAsStream("metadata/vdb/test2.json"), "UTF-8"),
                                VdbLoadTableConfig.class));
    }

    @Test(groups = "functional")
    public void mergeTest() {
        Table test1Table = vdbDataFeedMetadataService.getMetadata(vdbMetadata1, "Account").getLeft();
        Table test2Table = vdbDataFeedMetadataService.getMetadata(vdbMetadata2, "Account").getLeft();

        Table schemaTable1 = SchemaRepository.instance().getSchema(BusinessEntity.Account);
        Table schemaTable2 = SchemaRepository.instance().getSchema(BusinessEntity.Account);
        Table resolved1 = vdbDataFeedMetadataService.resolveMetadata(test1Table, schemaTable1);
        Table resolved2 = vdbDataFeedMetadataService.resolveMetadata(test2Table, schemaTable2);
        Assert.assertNotNull(resolved2.getAttribute(InterfaceName.CompanyName));
        Table mergedTable = dataFeedTaskManagerService.mergeTable(resolved2, resolved1);
        Assert.assertNotNull(mergedTable.getAttribute(InterfaceName.CompanyName));
        Assert.assertNotNull(mergedTable.getAttribute(InterfaceName.CompanyName).getSourceAttrName());
    }

    @Test(groups = "functional", expectedExceptions = RuntimeException.class)
    public void testGetMetadata() {
        testTable = vdbDataFeedMetadataService.getMetadata(testVdbMetadata, "Account").getLeft();
        Assert.assertNotNull(testTable);
        Assert.assertEquals(testTable.getName(), "FS_Data_For_Dante_Accounts_1000");
        Attribute requiredField = testTable.getAttribute("account");
        Assert.assertNotNull(requiredField);
        Attribute zipField = testTable.getAttribute("zip");
        Attribute urlField = testTable.getAttribute("url");
        Attribute verticalField = testTable.getAttribute("vertical");
        Assert.assertNotNull(zipField);
        Assert.assertNotNull(urlField);
        Assert.assertNotNull(verticalField);
        Assert.assertNull(zipField.getGroupsAsList());
        Assert.assertEquals(urlField.getGroupsAsList(), Arrays.asList(ColumnSelection.Predefined.CompanyProfile));
        Assert.assertEquals(verticalField.getGroupsAsList(),
                Arrays.asList(ColumnSelection.Predefined.TalkingPoint, ColumnSelection.Predefined.CompanyProfile));
        Table error = vdbDataFeedMetadataService.getMetadata(errorVdbMetadata, "Account").getLeft();
    }

    @Test(groups = "functional")
    public void testGetCustomerSpace() {
        CustomerSpace customerSpace = vdbDataFeedMetadataService.getCustomerSpace(testVdbMetadata);
        Assert.assertNotNull(customerSpace);
    }

    @Test(groups = "functional")
    public void testGetConnectorConfig() {
        String jobIdentifier = "Vdb_Metadata_Functional_Test";
        DataFeedTask dataFeedTask = new DataFeedTask();
        dataFeedTask.setUniqueId(jobIdentifier);
        String connectorConfig = vdbDataFeedMetadataService.getConnectorConfig(testVdbMetadata, dataFeedTask);
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
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        vdbDataFeedMetadataService.autoSetCDLExternalSystem(cdlExternalSystemService, resolvedTable, customerSpace);
        CDLExternalSystem system = cdlExternalSystemService.getExternalSystem(customerSpace,
                BusinessEntity.Account);
        Assert.assertNotNull(system);
        Assert.assertEquals(system.getCRMIdList().size(), 2);
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

    @Test(groups = "functional")
    public void testGetImportFileInfo() {
        CSVImportFileInfo csvImportFileInfo = vdbDataFeedMetadataService.getImportFileInfo(testVdbMetadata);
        Assert.assertNotNull(csvImportFileInfo);
        Assert.assertEquals(csvImportFileInfo.getFileUploadInitiator(), CDLConstants.DEFAULT_VISIDB_USER);
        Assert.assertNotNull(csvImportFileInfo.getReportFileDisplayName());
        Assert.assertNotNull(csvImportFileInfo.getReportFileName());
        log.info(String.format("fileName=%s, fileDisplayName=%s, initiator=%s", csvImportFileInfo.getReportFileName(),
                csvImportFileInfo.getReportFileDisplayName(), csvImportFileInfo.getFileUploadInitiator()));
    }

}
