package com.latticeengines.pls.util;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem.SystemType;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.pls.service.CDLService;

public class SystemIdsUtilsUnitTestNG {
    private static final Logger log = LoggerFactory.getLogger(SystemIdsUtilsUnitTestNG.class);

    private static String defaultSystemName = "DefaultSystem";
    private static String salesforceSystemName = "SalesforceSystem";
    private static String marketoSystemName = "MarketoSystem";
    private static String bomboraSystemName = "BomboraSystem";
    private static String dataWarehouseSystemName = "DataWarehouseSystem";


    @Test(groups = "unit")
    public void testProcessUniqueId_contactsWithMatchIds1() throws IOException {
        CustomerSpace customerSpace = new CustomerSpace("test", "test", "1");

        EntityType entityType = EntityType.Contacts;
        FieldDefinitionsRecord actualRecord = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-system-ids-contact-test1-record.json",
                FieldDefinitionsRecord.class);
        CDLService mockCdlService = Mockito.mock(CDLService.class);

        S3ImportSystem defaultImportSystem = new S3ImportSystem();
        defaultImportSystem.setName(defaultSystemName);
        defaultImportSystem.setAccountSystemId("user_" + defaultSystemName + "_11111111_AccountId");
        defaultImportSystem.setMapToLatticeAccount(true);
        defaultImportSystem.setContactSystemId("user_" + defaultSystemName + "_11111111_ContactId");
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), defaultSystemName))
                .thenReturn(defaultImportSystem);

        S3ImportSystem salesforceImportSystem = new S3ImportSystem();
        salesforceImportSystem.setName(salesforceSystemName);
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), salesforceSystemName)).thenReturn(
                salesforceImportSystem);

        S3ImportSystem marketoImportSystem = new S3ImportSystem();
        marketoImportSystem.setName(marketoSystemName);
        marketoImportSystem.setContactSystemId("user_" + marketoSystemName + "_33333333_ContactId");
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), marketoSystemName)).thenReturn(
                marketoImportSystem);

        S3ImportSystem bomboraImportSystem = new S3ImportSystem();
        bomboraImportSystem.setName(bomboraSystemName);
        bomboraImportSystem.setContactSystemId("user_" + bomboraSystemName + "_66666666_ContactId");
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), bomboraSystemName)).thenReturn(
                bomboraImportSystem);

        SystemIdsUtils.processSystemIds(customerSpace, salesforceSystemName, SystemType.Salesforce.name(), entityType,
                actualRecord, mockCdlService);

        log.info("Actual FieldDefinitionsRecord is:\n" + JsonUtils.pprint(actualRecord));
        log.info("DefaultImportSystem:\n" + JsonUtils.pprint(defaultImportSystem));
        log.info("SalesforceImportSystem:\n" + JsonUtils.pprint(salesforceImportSystem));
        log.info("MarketoImportSystem:\n" + JsonUtils.pprint(marketoImportSystem));
        log.info("BomboraImportSystem:\n" + JsonUtils.pprint(bomboraImportSystem));

        FieldDefinitionsRecord expectedRecord = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/expected-system-ids-contact-test1-record.json",
                FieldDefinitionsRecord.class);
        // Address random string in autogenerated fieldNames.
        List<FieldDefinition> actualUniqueIdDefinitionList = actualRecord.getFieldDefinitionsRecords(
                SystemIdsUtils.UNIQUE_ID_SECTION);
        assertEquals(actualUniqueIdDefinitionList.size(), 1);
        String actualUniqueIdFieldName = actualUniqueIdDefinitionList.get(0).getFieldName();
        assertTrue(actualUniqueIdFieldName.matches("user_SalesforceSystem_(\\w{8})_ContactId"));
        List<FieldDefinition> expectedUniqueIdDefinitionList = expectedRecord.getFieldDefinitionsRecords(
                SystemIdsUtils.UNIQUE_ID_SECTION);
        assertEquals(expectedUniqueIdDefinitionList.size(), 1);
        expectedUniqueIdDefinitionList.get(0).setFieldName(actualUniqueIdFieldName);

        Assert.assertEquals(actualRecord, expectedRecord,
                "Actual Record:\n" + JsonUtils.pprint(actualRecord) + "\nvs\n\nExpected Record:\n" +
                        JsonUtils.pprint(expectedRecord));
    }

    @Test(groups = "unit")
    public void testProcessUniqueId_contactsWithMatchIds2() throws IOException {
        CustomerSpace customerSpace = new CustomerSpace("test", "test", "1");

        EntityType entityType = EntityType.Contacts;
        FieldDefinitionsRecord actualRecord = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-system-ids-contact-test2-record.json",
                FieldDefinitionsRecord.class);
        CDLService mockCdlService = Mockito.mock(CDLService.class);

        S3ImportSystem defaultImportSystem = new S3ImportSystem();
        defaultImportSystem.setName(defaultSystemName);
        defaultImportSystem.setContactSystemId("user_" + defaultSystemName + "_11111111_ContactId");
        defaultImportSystem.setMapToLatticeContact(true);
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), defaultSystemName))
                .thenReturn(defaultImportSystem);

        S3ImportSystem salesforceImportSystem = new S3ImportSystem();
        salesforceImportSystem.setName(salesforceSystemName);
        salesforceImportSystem.setMapToLatticeAccount(true);
        salesforceImportSystem.setAccountSystemId("user_" + salesforceSystemName + "_22222222_AccountId");
        salesforceImportSystem.setContactSystemId("user_" + salesforceSystemName + "_22222222_ContactId");
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), salesforceSystemName)).thenReturn(
                salesforceImportSystem);

        S3ImportSystem marketoImportSystem = new S3ImportSystem();
        marketoImportSystem.setName(marketoSystemName);
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), marketoSystemName)).thenReturn(
                marketoImportSystem);

        S3ImportSystem bomboraImportSystem = new S3ImportSystem();
        bomboraImportSystem.setName(bomboraSystemName);
        bomboraImportSystem.setContactSystemId("user_" + bomboraSystemName + "_66666666_ContactId");
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), bomboraSystemName)).thenReturn(
                bomboraImportSystem);

        SystemIdsUtils.processSystemIds(customerSpace, marketoSystemName, SystemType.Marketo.name(), entityType,
                actualRecord, mockCdlService);

        log.info("Actual FieldDefinitionsRecord is:\n" + JsonUtils.pprint(actualRecord));
        log.info("DefaultImportSystem:\n" + JsonUtils.pprint(defaultImportSystem));
        log.info("SalesforceImportSystem:\n" + JsonUtils.pprint(salesforceImportSystem));
        log.info("MarketoImportSystem:\n" + JsonUtils.pprint(marketoImportSystem));
        log.info("BomboraImportSystem:\n" + JsonUtils.pprint(bomboraImportSystem));

        FieldDefinitionsRecord expectedRecord = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/expected-system-ids-contact-test2-record.json",
                FieldDefinitionsRecord.class);
        // Address random string in autogenerated fieldNames.
        List<FieldDefinition> actualUniqueIdDefinitionList = actualRecord.getFieldDefinitionsRecords(
                SystemIdsUtils.UNIQUE_ID_SECTION);
        assertEquals(actualUniqueIdDefinitionList.size(), 1);
        String actualUniqueIdFieldName = actualUniqueIdDefinitionList.get(0).getFieldName();
        assertTrue(actualUniqueIdFieldName.matches("user_MarketoSystem_(\\w{8})_ContactId"));
        List<FieldDefinition> expectedUniqueIdDefinitionList = expectedRecord.getFieldDefinitionsRecords(
                SystemIdsUtils.UNIQUE_ID_SECTION);
        assertEquals(expectedUniqueIdDefinitionList.size(), 1);
        expectedUniqueIdDefinitionList.get(0).setFieldName(actualUniqueIdFieldName);

        Assert.assertEquals(actualRecord, expectedRecord,
                "Actual Record:\n" + JsonUtils.pprint(actualRecord) + "\nvs\n\nExpected Record:\n" +
                        JsonUtils.pprint(expectedRecord));
    }

    @Test(groups = "unit")
    public void testProcessUniqueId_contactsWithMissingSystem() throws IOException {
        CustomerSpace customerSpace = new CustomerSpace("test", "test", "1");

        EntityType entityType = EntityType.Contacts;
        FieldDefinitionsRecord actualRecord = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-system-ids-contact-missing-system-record.json",
                FieldDefinitionsRecord.class);
        CDLService mockCdlService = Mockito.mock(CDLService.class);

        S3ImportSystem defaultImportSystem = new S3ImportSystem();
        defaultImportSystem.setName(defaultSystemName);
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), defaultSystemName)).thenReturn(
                defaultImportSystem);

        S3ImportSystem salesforceImportSystem = new S3ImportSystem();
        salesforceImportSystem.setName(salesforceSystemName);
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), salesforceSystemName)).thenReturn(
                salesforceImportSystem);

        S3ImportSystem bomboraImportSystem = new S3ImportSystem();
        bomboraImportSystem.setName(bomboraSystemName);
        bomboraImportSystem.setAccountSystemId("user_" + bomboraSystemName + "_55555555_AccountId");
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), bomboraSystemName)).thenReturn(
                bomboraImportSystem);

        IllegalStateException exception = Assert.expectThrows(IllegalStateException.class,
                () -> SystemIdsUtils.processSystemIds(customerSpace, defaultSystemName, SystemType.Other.name(),
                        entityType, actualRecord, mockCdlService));
        assertTrue(exception.getMessage().contains(
                "Cannot assign column Salesforce Contact Id as Contact ID from system " +
                        salesforceSystemName + " as match ID in section " + SystemIdsUtils.MATCH_IDS_SECTION +
                        " before that system has been set up"));

        log.info("Actual FieldDefinitionsRecord is:\n" + JsonUtils.pprint(actualRecord));
        log.info("DefaultImportSystem:\n" + JsonUtils.pprint(defaultImportSystem));
        log.info("SalesforceImportSystem:\n" + JsonUtils.pprint(salesforceImportSystem));
        log.info("BomboraImportSystem:\n" + JsonUtils.pprint(bomboraImportSystem));

        FieldDefinitionsRecord expectedRecord = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/expected-system-ids-contact-missing-system-record.json",
                FieldDefinitionsRecord.class);
        // Address random string in autogenerated fieldNames.
        List<FieldDefinition> actualUniqueIdDefinitionList = actualRecord.getFieldDefinitionsRecords(
                SystemIdsUtils.UNIQUE_ID_SECTION);
        assertEquals(actualUniqueIdDefinitionList.size(), 1);
        String actualUniqueIdFieldName = actualUniqueIdDefinitionList.get(0).getFieldName();
        assertTrue(actualUniqueIdFieldName.matches("user_DefaultSystem_(\\w{8})_ContactId"));
        List<FieldDefinition> expectedUniqueIdDefinitionList = expectedRecord.getFieldDefinitionsRecords(
                SystemIdsUtils.UNIQUE_ID_SECTION);
        assertEquals(expectedUniqueIdDefinitionList.size(), 1);
        expectedUniqueIdDefinitionList.get(0).setFieldName(actualUniqueIdFieldName);

        Assert.assertEquals(actualRecord, expectedRecord,
                "Actual Record:\n" + JsonUtils.pprint(actualRecord) + "\nvs\n\nExpected Record:\n" +
                        JsonUtils.pprint(expectedRecord));
    }

    @Test(groups = "unit")
    public void testProcessUniqueId_accountsWithMatchIds() throws IOException {
        CustomerSpace customerSpace = new CustomerSpace("test", "test", "1");

        EntityType entityType = EntityType.Accounts;
        FieldDefinitionsRecord actualRecord = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-system-ids-account-record.json",
                FieldDefinitionsRecord.class);
        CDLService mockCdlService = Mockito.mock(CDLService.class);

        S3ImportSystem defaultImportSystem = new S3ImportSystem();
        defaultImportSystem.setName(defaultSystemName);
        defaultImportSystem.setAccountSystemId("user_" + defaultSystemName + "_88888888_AccountId");
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), defaultSystemName)).thenReturn(
                defaultImportSystem);

        S3ImportSystem salesforceImportSystem = new S3ImportSystem();
        salesforceImportSystem.setName(salesforceSystemName);
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), salesforceSystemName)).thenReturn(
                salesforceImportSystem);

        S3ImportSystem dataWarehouseImportSystem = new S3ImportSystem();
        dataWarehouseImportSystem.setName(dataWarehouseSystemName);
        dataWarehouseImportSystem.setAccountSystemId("user_" + dataWarehouseSystemName + "_44444444_AccountId");
        dataWarehouseImportSystem.setMapToLatticeAccount(true);
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), dataWarehouseSystemName)).thenReturn(
                dataWarehouseImportSystem);

        S3ImportSystem bomboraImportSystem = new S3ImportSystem();
        bomboraImportSystem.setName(bomboraSystemName);
        bomboraImportSystem.setAccountSystemId("user_" + bomboraSystemName + "_55555555_AccountId");
        when(mockCdlService.getS3ImportSystem(customerSpace.toString(), bomboraSystemName)).thenReturn(
                bomboraImportSystem);

        SystemIdsUtils.processSystemIds(customerSpace, salesforceSystemName, SystemType.Salesforce.name(), entityType,
                actualRecord, mockCdlService);

        log.info("Actual FieldDefinitionsRecord is:\n" + JsonUtils.pprint(actualRecord));
        log.info("DefaultImportSystem:\n" + JsonUtils.pprint(defaultImportSystem));
        log.info("SalesforceImportSystem:\n" + JsonUtils.pprint(salesforceImportSystem));
        log.info("DataWarehouseImportSystem:\n" + JsonUtils.pprint(dataWarehouseImportSystem));
        log.info("BomboraImportSystem:\n" + JsonUtils.pprint(bomboraImportSystem));

        FieldDefinitionsRecord expectedRecord = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/expected-system-ids-account-record.json",
                FieldDefinitionsRecord.class);
        // Address random string in autogenerated fieldNames.
        List<FieldDefinition> actualUniqueIdDefinitionList = actualRecord.getFieldDefinitionsRecords(
                SystemIdsUtils.UNIQUE_ID_SECTION);
        assertEquals(actualUniqueIdDefinitionList.size(), 1);
        String actualUniqueIdFieldName = actualUniqueIdDefinitionList.get(0).getFieldName();
        assertTrue(actualUniqueIdFieldName.matches("user_SalesforceSystem_(\\w{8})_AccountId"));
        List<FieldDefinition> expectedUniqueIdDefinitionList = expectedRecord.getFieldDefinitionsRecords(
                SystemIdsUtils.UNIQUE_ID_SECTION);
        assertEquals(expectedUniqueIdDefinitionList.size(), 1);
        expectedUniqueIdDefinitionList.get(0).setFieldName(actualUniqueIdFieldName);

        Assert.assertEquals(actualRecord, expectedRecord,
                "Actual Record:\n" + JsonUtils.pprint(actualRecord) + "\nvs\n\nExpected Record:\n" +
                        JsonUtils.pprint(expectedRecord));
    }
}
