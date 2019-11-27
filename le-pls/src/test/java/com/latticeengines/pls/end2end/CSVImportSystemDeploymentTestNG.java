package com.latticeengines.pls.end2end;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.pls.service.CDLService;

/*
 * dpltc deploy -a pls,admin,cdl,lp,metadata
 */
public class CSVImportSystemDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    @Inject
    private CDLService cdlService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String featureFlag = LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG, flags);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testImportSystem() {
        // verify there's default system
        List<S3ImportSystem> allSystems = cdlService.getAllS3ImportSystem(mainTestTenant.getId());
        Assert.assertEquals(allSystems.size(), 1);
        S3ImportSystem defaultSystem = allSystems.get(0);
        Assert.assertTrue(defaultSystem.isPrimarySystem());
        Assert.assertNull(defaultSystem.getAccountSystemId());
        // create 2 new systems
        cdlService.createS3ImportSystem(mainTestTenant.getId(), "Test_SalesforceSystem",
                S3ImportSystem.SystemType.Other, false);
        cdlService.createS3ImportSystem(mainTestTenant.getId(), "Test_OtherSystem",
                S3ImportSystem.SystemType.Other, false);
        allSystems = cdlService.getAllS3ImportSystem(mainTestTenant.getId());
        Assert.assertEquals(allSystems.size(), 3);
        String sfSystemName = null, otherSystemName = null;
        for (S3ImportSystem system : allSystems) {
            if (system.getDisplayName().equals("Test_SalesforceSystem")) {
                sfSystemName = system.getName();
            } else if (system.getDisplayName().equals("Test_OtherSystem")) {
                otherSystemName = system.getName();
            }
        }
        Assert.assertFalse(StringUtils.isEmpty(sfSystemName));
        Assert.assertFalse(StringUtils.isEmpty(otherSystemName));

        S3ImportSystem sfSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), sfSystemName);
        S3ImportSystem otherSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), otherSystemName);
        Assert.assertNotNull(sfSystem);
        Assert.assertNotNull(otherSystem);

        // create template
        SourceFile defaultAccountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));
        String defaultFeedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT);

        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(defaultAccountFile.getName(), ENTITY_ACCOUNT, SOURCE, defaultFeedType);

        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("CrmAccount_External_ID")) {
                fieldMapping.setIdType(FieldMapping.IdType.Account);
            }
        }

        modelingFileMetadataService.resolveMetadata(defaultAccountFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                defaultFeedType);
        defaultAccountFile = sourceFileService.findByName(defaultAccountFile.getName());

        String defaultDFId = cdlService.createS3Template(customerSpace, defaultAccountFile.getName(),
                SOURCE, ENTITY_ACCOUNT, defaultFeedType, null, ENTITY_ACCOUNT + "Data");
        Assert.assertNotNull(defaultAccountFile);
        Assert.assertNotNull(defaultDFId);

        defaultSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), DEFAULT_SYSTEM);
        Assert.assertNotNull(defaultSystem);
        Assert.assertNotNull(defaultSystem.getAccountSystemId());
        Table defaultAccountTable = dataFeedProxy.getDataFeedTask(customerSpace, defaultDFId).getImportTemplate();
        Assert.assertNotNull(defaultAccountTable);
        Assert.assertNull(defaultAccountTable.getAttribute(InterfaceName.CustomerAccountId));

        // salesforce system with match account it to default system.
        SourceFile sfAccountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));
        String sfFeedType = getFeedTypeByEntity(sfSystemName, ENTITY_ACCOUNT);
        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sfAccountFile.getName(), ENTITY_ACCOUNT, SOURCE, sfFeedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("CrmAccount_External_ID")) {
                fieldMapping.setSystemName(DEFAULT_SYSTEM);
                fieldMapping.setIdType(FieldMapping.IdType.Account);
            }
            if (fieldMapping.getUserField().equals("ID")) {
                fieldMapping.setIdType(FieldMapping.IdType.Account);
                fieldMapping.setMapToLatticeId(true);
            }
            //remove id field.
            if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())) {
                fieldMapping.setMappedField(null);
            }
        }
        modelingFileMetadataService.resolveMetadata(sfAccountFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                sfFeedType);
        sfAccountFile = sourceFileService.findByName(sfAccountFile.getName());

        String sfDFId = cdlService.createS3Template(customerSpace, sfAccountFile.getName(),
                SOURCE, ENTITY_ACCOUNT, sfFeedType, null, ENTITY_ACCOUNT + "Data");
        Assert.assertNotNull(sfAccountFile);
        Assert.assertNotNull(sfDFId);

        // other system with match account it to itself.
        SourceFile otherAccountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));
        String otherFeedType = getFeedTypeByEntity(otherSystemName, ENTITY_ACCOUNT);
        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(otherAccountFile.getName(), ENTITY_ACCOUNT, SOURCE, otherFeedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("CrmAccount_External_ID")) {
                fieldMapping.setIdType(FieldMapping.IdType.Account);
            }
        }
        modelingFileMetadataService.resolveMetadata(otherAccountFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                otherFeedType);
        otherAccountFile = sourceFileService.findByName(otherAccountFile.getName());

        String otherDFId = cdlService.createS3Template(customerSpace, otherAccountFile.getName(),
                SOURCE, ENTITY_ACCOUNT, otherFeedType, null, ENTITY_ACCOUNT + "Data");
        Assert.assertNotNull(otherAccountFile);
        Assert.assertNotNull(otherDFId);

        allSystems = cdlService.getAllS3ImportSystem(mainTestTenant.getId());
        Assert.assertEquals(allSystems.size(), 3);
        defaultSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), DEFAULT_SYSTEM);
        Assert.assertFalse(defaultSystem.isPrimarySystem());
        sfSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), sfSystemName);
        Assert.assertTrue(sfSystem.isPrimarySystem());
        Assert.assertNotNull(sfSystem.getAccountSystemId());
        Assert.assertTrue(sfSystem.isMapToLatticeAccount());
        Table sfAccountTable = dataFeedProxy.getDataFeedTask(customerSpace, sfDFId).getImportTemplate();
        Assert.assertNotNull(sfAccountTable);
        Assert.assertNotNull(sfAccountTable.getAttribute(defaultSystem.getAccountSystemId()));
        Attribute customerAccountId = sfAccountTable.getAttribute(InterfaceName.CustomerAccountId);
        Assert.assertNotNull(customerAccountId);
        Assert.assertEquals(customerAccountId.getDisplayName(), "ID");
        Attribute sfSystemIdAttr = sfAccountTable.getAttribute(sfSystem.getAccountSystemId());
        Assert.assertNotNull(sfSystemIdAttr);
        Assert.assertEquals(sfSystemIdAttr.getDisplayName(), "ID");
        otherSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), otherSystemName);
        Assert.assertNotNull(otherSystem.getAccountSystemId());
        Table otherSystemAccountTable =
                dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), otherDFId).getImportTemplate();
        Attribute otherSystemAccountAttr = otherSystemAccountTable.getAttribute(otherSystem.getAccountSystemId());
        Assert.assertNotNull(otherSystemAccountAttr);
        Assert.assertEquals(otherSystemAccountAttr.getDisplayName(), "CrmAccount_External_ID");

        // test with contact
        SourceFile sfContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        String sfContactFeedType = getFeedTypeByEntity(sfSystemName, ENTITY_CONTACT);
        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sfContactFile.getName(), ENTITY_CONTACT, SOURCE, sfContactFeedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("S_Contact_For_PlatformTest")) {
                fieldMapping.setIdType(FieldMapping.IdType.Contact);
                fieldMapping.setMapToLatticeId(true);
                fieldMapping.setMappedToLatticeField(true);
            }
            if (fieldMapping.getUserField().equals("Account_ID")) {
                fieldMapping.setIdType(FieldMapping.IdType.Account);
                fieldMapping.setMappedToLatticeField(true);
            }
        }
        modelingFileMetadataService.resolveMetadata(sfContactFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                sfContactFeedType);
        sfContactFile = sourceFileService.findByName(sfContactFile.getName());

        String sfContactDFId = cdlService.createS3Template(customerSpace, sfContactFile.getName(),
                SOURCE, ENTITY_CONTACT, sfContactFeedType, null, ENTITY_CONTACT + "Data");
        Assert.assertNotNull(sfContactFile);
        Assert.assertNotNull(sfContactDFId);


        SourceFile otherContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        String otherContactFeedType = getFeedTypeByEntity(otherSystemName, ENTITY_CONTACT);
        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(otherContactFile.getName(), ENTITY_CONTACT, SOURCE, otherContactFeedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("S_Contact_For_PlatformTest")) {
                fieldMapping.setSystemName(sfSystemName);
                fieldMapping.setIdType(FieldMapping.IdType.Contact);
            }
            if (fieldMapping.getUserField().equals("Account_ID")) {
                fieldMapping.setSystemName(sfSystemName);
                fieldMapping.setIdType(FieldMapping.IdType.Account);
            }
        }
        modelingFileMetadataService.resolveMetadata(otherContactFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                otherContactFeedType);
        otherContactFile = sourceFileService.findByName(otherContactFile.getName());

        String otherContactDFId = cdlService.createS3Template(customerSpace, otherContactFile.getName(),
                SOURCE, ENTITY_CONTACT, otherContactFeedType, null, ENTITY_CONTACT + "Data");
        Assert.assertNotNull(otherContactFile);
        Assert.assertNotNull(otherContactDFId);

        sfSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), sfSystemName);
        Assert.assertNotNull(sfSystem.getContactSystemId());
        Table sfSystemContactTable =
                dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), sfContactDFId).getImportTemplate();
        Attribute sfSystemContactIdAttr = sfSystemContactTable.getAttribute(sfSystem.getContactSystemId());
        Assert.assertNotNull(sfSystemContactIdAttr);
        Assert.assertEquals(sfSystemContactIdAttr.getDisplayName(), "S_Contact_For_PlatformTest");
        otherSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), otherSystemName);
        Assert.assertNull(otherSystem.getContactSystemId());

        Table otherSystemContactTable =
                dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), otherContactDFId).getImportTemplate();
        Attribute otherSystemContactIdAttr = otherSystemContactTable.getAttribute(sfSystem.getContactSystemId());
        Assert.assertNotNull(otherSystemContactIdAttr);
        Assert.assertEquals(otherSystemContactIdAttr.getDisplayName(), "S_Contact_For_PlatformTest");

        Attribute otherSystemAccountIdAttr = otherSystemContactTable.getAttribute(sfSystem.getAccountSystemId());
        Assert.assertNotNull(otherSystemAccountIdAttr);
        Assert.assertEquals(otherSystemAccountIdAttr.getDisplayName(), "Account_ID");

        Attribute otherSystemCustomerAccountIdAttr =
                otherSystemContactTable.getAttribute(InterfaceName.CustomerAccountId);
        Assert.assertNotNull(otherSystemCustomerAccountIdAttr);
        Assert.assertEquals(otherSystemCustomerAccountIdAttr.getDisplayName(), "Account_ID");

        // check upload file again and can get system name this time
        otherContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        otherContactFeedType = getFeedTypeByEntity(otherSystemName, ENTITY_CONTACT);
        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(otherContactFile.getName(), ENTITY_CONTACT, SOURCE, otherContactFeedType);
        Map<String, FieldMapping> fieldMappingMapFromReImport =
                fieldMappingDocument.getFieldMappings().stream()
                        .filter(fieldMapping -> StringUtils.isNotEmpty(fieldMapping.getMappedField()))
                        .collect(Collectors.toMap(FieldMapping::getMappedField, fieldMapping -> fieldMapping));
        Assert.assertTrue(fieldMappingMapFromReImport.containsKey(sfSystem.getAccountSystemId()));
        Assert.assertTrue(fieldMappingMapFromReImport.containsKey(sfSystem.getContactSystemId()));
        Assert.assertEquals(fieldMappingMapFromReImport.get(sfSystem.getAccountSystemId()).getSystemName(),
                sfSystem.getName());
        Assert.assertEquals(fieldMappingMapFromReImport.get(sfSystem.getContactSystemId()).getSystemName(),
                sfSystem.getName());
        Assert.assertEquals(fieldMappingMapFromReImport.get(sfSystem.getAccountSystemId()).getIdType(),
                FieldMapping.IdType.Account);
        Assert.assertEquals(fieldMappingMapFromReImport.get(sfSystem.getContactSystemId()).getIdType(),
                FieldMapping.IdType.Contact);


        // exception when double primary system.
        defaultAccountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));

        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(defaultAccountFile.getName(), ENTITY_ACCOUNT, SOURCE, defaultFeedType);

        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("CrmAccount_External_ID")) {
                fieldMapping.setIdType(FieldMapping.IdType.Account);
                fieldMapping.setMapToLatticeId(true);
            }
        }

        SourceFile finalDefaultAccountFile = defaultAccountFile;
        FieldMappingDocument finalFieldMappingDocument = fieldMappingDocument;
        Assert.expectThrows(LedpException.class,
                () -> modelingFileMetadataService.resolveMetadata(finalDefaultAccountFile.getName(),
                        finalFieldMappingDocument, ENTITY_ACCOUNT, SOURCE, defaultFeedType));

    }

    @Test(groups = "deployment", dependsOnMethods = "testImportSystem")
    public void testMapToLatticeIdFlag() {
        SourceFile sfAccountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));
        String sfFeedType = getFeedTypeByEntity("Test_SalesforceSystem", ENTITY_ACCOUNT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sfAccountFile.getName(), ENTITY_ACCOUNT, SOURCE, sfFeedType);
        boolean hasCustomerAccountId = false;
        int idMappingCount = 0;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())) {
                Assert.assertTrue(fieldMapping.isMapToLatticeId());
                hasCustomerAccountId = true;
            }
            if (fieldMapping.getUserField().equals("ID")) {
                idMappingCount++;
            }
        }
        Assert.assertTrue(hasCustomerAccountId);
        Assert.assertTrue(idMappingCount > 1);
    }

    @Test(groups = "deployment", dependsOnMethods = "testMapToLatticeIdFlag")
    public void testPriorityList() {
        List<S3ImportSystem> allSystems = cdlService.getAllS3ImportSystem(mainTestTenant.getId());
        Assert.assertEquals(allSystems.size(), 3);
        Assert.assertEquals(allSystems.get(0).getPriority(), 1);
        Assert.assertEquals(allSystems.get(1).getPriority(), 2);
        Assert.assertEquals(allSystems.get(2).getPriority(), 3);
    }

    @Test(groups = "deployment", dependsOnMethods = "testPriorityList")
    public void testMultipleSubType() {
        List<S3ImportSystem> allSystems = cdlService.getAllS3ImportSystem(mainTestTenant.getId());
        Assert.assertEquals(allSystems.size(), 3);
        cdlService.createS3ImportSystem(mainTestTenant.getId(), "Test_SalesforceSystemLead",
                S3ImportSystem.SystemType.Salesforce, false);
        allSystems = cdlService.getAllS3ImportSystem(mainTestTenant.getId());
        String sfSystemName = null;
        for (S3ImportSystem system : allSystems) {
            if (system.getDisplayName().equals("Test_SalesforceSystemLead")) {
                sfSystemName = system.getName();
            }
        }
        Assert.assertFalse(StringUtils.isEmpty(sfSystemName));
        S3ImportSystem sfSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), sfSystemName);
        Assert.assertNotNull(sfSystem);
        SourceFile sfContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        String sfContactFeedType = getFeedTypeByEntity(sfSystemName, ENTITY_CONTACT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sfContactFile.getName(), ENTITY_CONTACT, SOURCE, sfContactFeedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("S_Contact_For_PlatformTest")) {
                fieldMapping.setIdType(FieldMapping.IdType.Contact);
                fieldMapping.setMappedToLatticeField(true);
            }
        }
        modelingFileMetadataService.resolveMetadata(sfContactFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                sfContactFeedType);
        sfContactFile = sourceFileService.findByName(sfContactFile.getName());

        String sfContactDFId = cdlService.createS3Template(customerSpace, sfContactFile.getName(),
                SOURCE, ENTITY_CONTACT, sfContactFeedType, null, ENTITY_CONTACT + "Data");
        Assert.assertNotNull(sfContactFile);
        Assert.assertNotNull(sfContactDFId);

        sfContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));

        String sfLeadFeedType = sfSystemName + "_LeadsData";
        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sfContactFile.getName(), ENTITY_CONTACT, SOURCE, sfLeadFeedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("S_Contact_For_PlatformTest")) {
                fieldMapping.setIdType(FieldMapping.IdType.Contact);
                fieldMapping.setMapToLatticeId(true);
            }
            if (fieldMapping.getUserField().equals("ID")) {
                fieldMapping.setIdType(FieldMapping.IdType.Contact);
                fieldMapping.setSystemName(sfSystemName);
            }
        }
        modelingFileMetadataService.resolveMetadata(sfContactFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                sfLeadFeedType);
        String sfLeadsDFId = cdlService.createS3Template(customerSpace, sfContactFile.getName(),
                SOURCE, ENTITY_CONTACT, sfLeadFeedType, DataFeedTask.SubType.Lead.name(), "LeadsData");
        Assert.assertNotNull(sfContactFile);
        Assert.assertNotNull(sfLeadsDFId);

        sfSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), sfSystemName);
        Assert.assertNotNull(sfSystem);

        Assert.assertNotNull(sfSystem.getSecondaryContactIds());
        Assert.assertTrue(StringUtils.isNotEmpty(sfSystem.getContactSystemId()));

        Table sfContactTable =
                dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), sfContactDFId).getImportTemplate();

        Table sfLeadTable = dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), sfLeadsDFId).getImportTemplate();

        Assert.assertNotNull(sfContactTable);
        Assert.assertNotNull(sfLeadTable);

        Assert.assertNotNull(sfContactTable.getAttribute(sfSystem.getContactSystemId()));
        Assert.assertNotNull(sfLeadTable.getAttribute(sfSystem.getSecondaryContactId(EntityType.Leads)));
        Assert.assertNotNull(sfLeadTable.getAttribute(sfSystem.getContactSystemId()));

    }

    @Test(groups = "deployment", dependsOnMethods = "testMultipleSubType")
    public void testGetSystemList() {
        // Right now there should be 4 systems: DefaultSystem, Test_SalesforceSystem, Test_OtherSystem, Test_SalesforceSystemLead
        // Three of them have Account System Id : DefaultSystem, Test_SalesforceSystem, Test_OtherSystem
        // Two of them have Contact System Id : Test_SalesforceSystem, Test_SalesforceSystemLead
        // One of them has Leads System Id(secondary Id) : Test_SalesforceSystemLead
        List<S3ImportTemplateDisplay> templateList = cdlService.getS3ImportTemplate(mainTestTenant.getId(), "", null);
        S3ImportTemplateDisplay otherSystemAccount =
                templateList.stream().filter(templateDisplay -> templateDisplay.getFeedType().equals(
                        "Test_OtherSystem_AccountData")).findFirst().get();
        Assert.assertNotNull(otherSystemAccount);
        List<S3ImportSystem> filteredS3ImportSystems = cdlService.getS3ImportSystemWithFilter(mainTestTenant.getId(),
                true, false, otherSystemAccount);
        Assert.assertEquals(filteredS3ImportSystems.size(), 2);

        S3ImportTemplateDisplay sfSystemContact =
                templateList.stream().filter(templateDisplay -> templateDisplay.getFeedType().equals(
                        "Test_SalesforceSystemLead_ContactData")).findFirst().get();
        filteredS3ImportSystems = cdlService.getS3ImportSystemWithFilter(mainTestTenant.getId(),
                false, true, sfSystemContact);
        Assert.assertEquals(filteredS3ImportSystems.size(), 1);

        filteredS3ImportSystems = cdlService.getS3ImportSystemWithFilter(mainTestTenant.getId(),
                true, false, sfSystemContact);
        Assert.assertEquals(filteredS3ImportSystems.size(), 3);

        S3ImportTemplateDisplay sfSystemLead =
                templateList.stream().filter(templateDisplay -> templateDisplay.getFeedType().equals(
                        "Test_SalesforceSystemLead_LeadsData")).findFirst().get();
        filteredS3ImportSystems = cdlService.getS3ImportSystemWithFilter(mainTestTenant.getId(),
                false, true, sfSystemLead);
        Assert.assertEquals(filteredS3ImportSystems.size(), 2);
    }

}
