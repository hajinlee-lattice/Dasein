package com.latticeengines.pls.end2end;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.pls.service.CDLService;

/*
 * dpltc deploy -a pls,admin,cdl,lp,metadata,workflowapi,matchapi
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
        createDefaultImportSystem();
    }

    @Test(groups = "deployment")
    public void testImportSystem() {
        // verify there's default system
        List<S3ImportSystem> allSystems = cdlService.getAllS3ImportSystem(mainTestTenant.getId());
        Assert.assertEquals(allSystems.size(), 1);
        S3ImportSystem defaultSystem = allSystems.get(0);
        Assert.assertEquals(defaultSystem.getPriority(), 1);
        Assert.assertNull(defaultSystem.getAccountSystemId());
        // create 2 new systems
        allSystems = createImportSystem();
        Assert.assertEquals(allSystems.size(), 3);
        String sfSystemName = null, otherSystemName = null;
        for (S3ImportSystem system : allSystems) {
            if (system.getDisplayName().equals("Test_SalesforceSystem")) {
                sfSystemName = system.getName();
            } else if (system.getDisplayName().equals("Test_OtherSystem")) {
                otherSystemName = system.getName();
            }
        }
        verifyImportSystem(sfSystemName, otherSystemName);

        // create template
        String defaultFeedType = createDefaultAccountTemplateAndVerify();

        // salesforce system with match account it to default system.
        String sfDFId = createSFAccountTemplateAndVerify(sfSystemName);

        // other system with match account it to itself.
        String otherDFId = createOtherSystemAccountAndVerify(otherSystemName);

        allSystems = cdlService.getAllS3ImportSystem(mainTestTenant.getId());
        defaultSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), DEFAULT_SYSTEM);
        S3ImportSystem sfSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), sfSystemName);
        S3ImportSystem otherSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), otherSystemName);
        verifySystemAfterCreateAccountTemplate(allSystems, defaultSystem, sfSystem, otherSystem, sfDFId, otherDFId);

        // test with contact
        String sfContactDFId = createSFContactTemplateAndVerify(sfSystemName);

        String otherContactDFId = createOtherContactTemplateAndVerify(sfSystemName, otherSystemName);

        sfSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), sfSystemName);
        otherSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), otherSystemName);

        verifySystemAfterCreateContactTemplate(sfSystem, otherSystem, sfContactDFId, otherContactDFId);

        // test with Transaction
        String otherTxnDFId = createOtherTransactionTemplateAndVerify(otherSystemName);

        verifySystemAfterCreateTxnTemplate(otherSystem, otherTxnDFId);

        // check upload file again and can get system name this time
        verifyEditTemplate(otherSystemName, sfSystem);

        // exception when double primary system.
        SourceFile defaultAccountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));

        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(defaultAccountFile.getName(), ENTITY_ACCOUNT, SOURCE, defaultFeedType);

        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("CrmAccount_External_ID")) {
                fieldMapping.setIdType(FieldMapping.IdType.Account);
                fieldMapping.setMapToLatticeId(true);
                fieldMapping.setMappedToLatticeSystem(false);
            }
        }

        Assert.expectThrows(LedpException.class,
                () -> modelingFileMetadataService.resolveMetadata(defaultAccountFile.getName(),
                        fieldMappingDocument, ENTITY_ACCOUNT, SOURCE, defaultFeedType));

    }

    private void verifyEditTemplate(String otherSystemName, S3ImportSystem sfSystem) {
        String otherContactDFId;
        SourceFile otherContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        String otherContactFeedType = getFeedTypeByEntity(otherSystemName, ENTITY_CONTACT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
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

        modelingFileMetadataService.resolveMetadata(otherContactFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                otherContactFeedType);
        otherContactFile = sourceFileService.findByName(otherContactFile.getName());
        otherContactDFId = cdlService.createS3Template(customerSpace, otherContactFile.getName(),
                SOURCE, ENTITY_CONTACT, otherContactFeedType, null, ENTITY_CONTACT + "Data");
        Assert.assertNotNull(otherContactFile);
        Assert.assertNotNull(otherContactDFId);
    }

    private void verifySystemAfterCreateContactTemplate(S3ImportSystem sfSystem, S3ImportSystem otherSystem, String sfContactDFId, String otherContactDFId) {
        Assert.assertNotNull(sfSystem.getContactSystemId());
        Table sfSystemContactTable =
                dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), sfContactDFId).getImportTemplate();
        Attribute sfSystemContactIdAttr = sfSystemContactTable.getAttribute(sfSystem.getContactSystemId());
        Assert.assertNotNull(sfSystemContactIdAttr);
        Assert.assertEquals(sfSystemContactIdAttr.getDisplayName(), "S_Contact_For_PlatformTest");

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
        Assert.assertEquals(otherSystemCustomerAccountIdAttr.getSourceAttrName(), "Account_ID");
        Assert.assertEquals(otherSystemCustomerAccountIdAttr.getDisplayName(), InterfaceName.CustomerAccountId.name());
    }

    private void verifySystemAfterCreateTxnTemplate(S3ImportSystem otherSystem, String otherTxnDFId) {
        Assert.assertNotNull(otherSystem.getAccountSystemId());
        Table otherSystemTxnTable =
                dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), otherTxnDFId).getImportTemplate();
        Assert.assertNotNull(otherSystemTxnTable);
        Attribute otherSystemAccountIdAttr = otherSystemTxnTable.getAttribute(otherSystem.getAccountSystemId());
        Assert.assertNotNull(otherSystemAccountIdAttr);
        Assert.assertEquals(otherSystemAccountIdAttr.getDisplayName(), "Account_ID");
    }

    private String createOtherContactTemplateAndVerify(String sfSystemName, String otherSystemName) {
        SourceFile otherContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        String otherContactFeedType = getFeedTypeByEntity(otherSystemName, ENTITY_CONTACT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
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
        return otherContactDFId;
    }

    private String createOtherTransactionTemplateAndVerify(String otherSystemName) {
        SourceFile otherTransactionFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_TRANSACTION), ENTITY_TRANSACTION, TRANSACTION_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + TRANSACTION_SOURCE_FILE));
        String otherTransactionFeedType = getFeedTypeByEntity(otherSystemName, ENTITY_TRANSACTION);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(otherTransactionFile.getName(), ENTITY_TRANSACTION, SOURCE, otherTransactionFeedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("Account_ID")) {
                fieldMapping.setSystemName(otherSystemName);
                fieldMapping.setIdType(FieldMapping.IdType.Account);
            }
        }
        modelingFileMetadataService.resolveMetadata(otherTransactionFile.getName(), fieldMappingDocument, ENTITY_TRANSACTION,
                SOURCE, otherTransactionFeedType);
        otherTransactionFile = sourceFileService.findByName(otherTransactionFile.getName());

        String otherTransactionDFId = cdlService.createS3Template(customerSpace, otherTransactionFile.getName(),
                SOURCE, ENTITY_TRANSACTION, otherTransactionFeedType, null, ENTITY_TRANSACTION + "Data");
        Assert.assertNotNull(otherTransactionFile);
        Assert.assertNotNull(otherTransactionDFId);
        return otherTransactionDFId;
    }

    private String createSFContactTemplateAndVerify(String sfSystemName) {
        SourceFile sfContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        String sfContactFeedType = getFeedTypeByEntity(sfSystemName, ENTITY_CONTACT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
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
        return sfContactDFId;
    }

    private void verifySystemAfterCreateAccountTemplate(List<S3ImportSystem> allSystems, S3ImportSystem defaultSystem,
                                                        S3ImportSystem sfSystem, S3ImportSystem otherSystem, String sfDFId, String otherDFId) {
        Table sfAccountTable = dataFeedProxy.getDataFeedTask(customerSpace, sfDFId).getImportTemplate();
        Attribute customerAccountId = sfAccountTable.getAttribute(InterfaceName.CustomerAccountId);
        Attribute sfSystemIdAttr = sfAccountTable.getAttribute(sfSystem.getAccountSystemId());
        Table otherSystemAccountTable =
                dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), otherDFId).getImportTemplate();
        Attribute otherSystemAccountAttr = otherSystemAccountTable.getAttribute(otherSystem.getAccountSystemId());

        Assert.assertEquals(allSystems.size(), 3);
        Assert.assertNotEquals(defaultSystem.getPriority(), 1);
        Assert.assertEquals(sfSystem.getPriority(), 1);
        Assert.assertNotNull(sfSystem.getAccountSystemId());
        Assert.assertTrue(sfSystem.isMapToLatticeAccount());
        Assert.assertNotNull(sfAccountTable);
        Assert.assertNotNull(sfAccountTable.getAttribute(defaultSystem.getAccountSystemId()));
        Assert.assertNotNull(customerAccountId);
        Assert.assertEquals(customerAccountId.getDisplayName(), InterfaceName.CustomerAccountId.name());
        Assert.assertEquals(customerAccountId.getSourceAttrName(), "ID");
        Assert.assertNotNull(sfSystemIdAttr);
        Assert.assertEquals(sfSystemIdAttr.getDisplayName(), "ID");
        Assert.assertNotNull(otherSystem.getAccountSystemId());
        Assert.assertNotNull(otherSystemAccountAttr);
        Assert.assertEquals(otherSystemAccountAttr.getDisplayName(), "CrmAccount_External_ID");
    }

    private String createOtherSystemAccountAndVerify(String otherSystemName) {
        SourceFile otherAccountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));
        String otherFeedType = getFeedTypeByEntity(otherSystemName, ENTITY_ACCOUNT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
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
        return otherDFId;
    }

    private String createSFAccountTemplateAndVerify(String sfSystemName) {
        SourceFile sfAccountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));
        String sfFeedType = getFeedTypeByEntity(sfSystemName, ENTITY_ACCOUNT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
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
        return sfDFId;
    }

    private String createDefaultAccountTemplateAndVerify() {
        S3ImportSystem defaultSystem;
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
        return defaultFeedType;
    }

    private void verifyImportSystem(String sfSystemName, String otherSystemName) {
        Assert.assertFalse(StringUtils.isEmpty(sfSystemName));
        Assert.assertFalse(StringUtils.isEmpty(otherSystemName));

        S3ImportSystem sfSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), sfSystemName);
        S3ImportSystem otherSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), otherSystemName);
        Assert.assertNotNull(sfSystem);
        Assert.assertNotNull(otherSystem);
    }

    private List<S3ImportSystem> createImportSystem() {
        List<S3ImportSystem> allSystems;
        cdlService.createS3ImportSystem(mainTestTenant.getId(), "Test_SalesforceSystem",
                S3ImportSystem.SystemType.Other, false);
        cdlService.createS3ImportSystem(mainTestTenant.getId(), "Test_OtherSystem",
                S3ImportSystem.SystemType.Other, false);
        allSystems = cdlService.getAllS3ImportSystem(mainTestTenant.getId());
        return allSystems;
    }

    @Test(groups = "deployment", dependsOnMethods = "testImportSystem")
    public void  testGetDataFeedTask() {
        List<DataFeedTask> accountTasks =
                dataFeedProxy.getDataFeedTaskWithSameEntity(customerSpace, BusinessEntity.Account.name());
        Assert.assertNotNull(accountTasks);
        Assert.assertEquals(accountTasks.size(), 3);
        String defaultFeedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT);
        // get data feed tasks excluding case souce is File and default feed type
        accountTasks =
                dataFeedProxy.getDataFeedTaskWithSameEntityExcludeOne(customerSpace, BusinessEntity.Account.name(),
                        SOURCE, defaultFeedType);
        Assert.assertNotNull(accountTasks);
        Assert.assertEquals(accountTasks.size(), 2);

    }

    @Test(groups = "deployment", dependsOnMethods = "testImportSystem")
    public void testMapToLatticeIdFlag() {
        SourceFile sfAccountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));
        String sfFeedType = getFeedTypeByEntity("Test_SalesforceSystem", ENTITY_ACCOUNT);
        S3ImportSystem sfSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), "Test_SalesforceSystem");
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sfAccountFile.getName(), ENTITY_ACCOUNT, SOURCE, sfFeedType);
        boolean hasCustomerAccountId = false;
        int idMappingCount = 0;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())) {
                hasCustomerAccountId = true;
            }
            if (sfSystem.getAccountSystemId().equals(fieldMapping.getMappedField())) {
                Assert.assertTrue(fieldMapping.isMapToLatticeId());
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

        // test edit template with Lead
        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sfContactFile.getName(), ENTITY_CONTACT, SOURCE, sfLeadFeedType);
        Map<String, FieldMapping> fieldMappingMapFromReImport =
                fieldMappingDocument.getFieldMappings().stream()
                        .filter(fieldMapping -> StringUtils.isNotEmpty(fieldMapping.getMappedField()))
                        .collect(Collectors.toMap(FieldMapping::getMappedField, fieldMapping -> fieldMapping));
        Assert.assertTrue(fieldMappingMapFromReImport.containsKey(sfSystem.getSecondaryContactId(EntityType.Leads)));
        Assert.assertEquals(fieldMappingMapFromReImport.get(sfSystem.getSecondaryContactId(EntityType.Leads)).getSystemName(),
                sfSystem.getName());
        Assert.assertEquals(fieldMappingMapFromReImport.get(sfSystem.getSecondaryContactId(EntityType.Leads)).getIdType(),
                FieldMapping.IdType.Lead);

    }

    @Test(groups = "deployment", dependsOnMethods = "testMultipleSubType")
    public void testGetSystemList() {
        // Right now there should be 4 systems: DefaultSystem, Test_SalesforceSystem, Test_OtherSystem, Test_SalesforceSystemLead
        // Three of them have Account System Id : DefaultSystem, Test_SalesforceSystem, Test_OtherSystem
        // Two of them have Contact System Id : Test_SalesforceSystem, Test_SalesforceSystemLead
        // One of them has Leads System Id(secondary Id) : Test_SalesforceSystemLead
        List<S3ImportTemplateDisplay> templateList = cdlService.getS3ImportTemplate(mainTestTenant.getId(), "", null);
        Optional<S3ImportTemplateDisplay> otherSystemAccountOpt = templateList.stream()
                .filter(templateDisplay -> templateDisplay.getFeedType().equals("Test_OtherSystem_AccountData"))
                .findFirst();
        Assert.assertTrue(otherSystemAccountOpt.isPresent());
        S3ImportTemplateDisplay otherSystemAccount = otherSystemAccountOpt.get();
        Assert.assertNotNull(otherSystemAccount);
        List<S3ImportSystem> filteredS3ImportSystems = cdlService.getS3ImportSystemWithFilter(mainTestTenant.getId(),
                true, false, otherSystemAccount);
        Assert.assertEquals(filteredS3ImportSystems.size(), 2);

        Optional<S3ImportTemplateDisplay> sfSystemContactOpt = templateList.stream()
                .filter(templateDisplay -> templateDisplay.getFeedType().equals("Test_SalesforceSystemLead_ContactData"))
                .findFirst();
        Assert.assertTrue(sfSystemContactOpt.isPresent());
        S3ImportTemplateDisplay sfSystemContact = sfSystemContactOpt.get();
        filteredS3ImportSystems = cdlService.getS3ImportSystemWithFilter(mainTestTenant.getId(),
                false, true, sfSystemContact);
        Assert.assertEquals(filteredS3ImportSystems.size(), 1);

        filteredS3ImportSystems = cdlService.getS3ImportSystemWithFilter(mainTestTenant.getId(),
                true, false, sfSystemContact);
        Assert.assertEquals(filteredS3ImportSystems.size(), 3);

        Optional<S3ImportTemplateDisplay> sfSystemLeadOpt = templateList.stream()
                .filter(templateDisplay -> templateDisplay.getFeedType().equals("Test_SalesforceSystemLead_LeadsData"))
                .findFirst();
        Assert.assertTrue(sfSystemLeadOpt.isPresent());
        S3ImportTemplateDisplay sfSystemLead = sfSystemLeadOpt.get();
        filteredS3ImportSystems = cdlService.getS3ImportSystemWithFilter(mainTestTenant.getId(),
                false, true, sfSystemLead);
        Assert.assertEquals(filteredS3ImportSystems.size(), 2);
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetSystemList")
    public void testMarketoSystem() {
        // 1. create Marketo system
        cdlService.createS3ImportSystem(mainTestTenant.getId(), "MKTO_System", S3ImportSystem.SystemType.Marketo,
                false);
        List<S3ImportSystem> systemList = cdlService.getAllS3ImportSystem(mainTestTenant.getId());
        Optional<S3ImportSystem> first = systemList.stream()
                .filter(s3ImportSystem ->
                        S3ImportSystem.SystemType.Marketo.equals(s3ImportSystem.getSystemType()) && "MKTO_System".equals(s3ImportSystem.getDisplayName()))
                .findFirst();
        Assert.assertTrue(first.isPresent());
        S3ImportSystem mktoSystem = first.get();
        String mktoLeadDFId = createMKTOContactTemplateAndVerify(mktoSystem.getName());
        mktoSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), mktoSystem.getName());
        Assert.assertNotNull(mktoSystem);
        Assert.assertFalse(StringUtils.isEmpty(mktoSystem.getContactSystemId()));
        Table mktoContact = dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), mktoLeadDFId).getImportTemplate();
        Assert.assertNotNull(mktoContact);
        Attribute leadId = mktoContact.getAttribute(mktoSystem.getContactSystemId());
        Assert.assertEquals(leadId.getDisplayName(), "S_Contact_For_PlatformTest");
    }

    private String createMKTOContactTemplateAndVerify(String mktoSystemName) {
        SourceFile mktSourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                CONTACT_SOURCE_FILE, EntityType.Leads,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        String mktLeadFeedType = mktoSystemName + SPLIT_CHART + EntityType.Leads.getDefaultFeedTypeName();

        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(mktSourceFile.getName(), ENTITY_CONTACT, SOURCE, mktLeadFeedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("S_Contact_For_PlatformTest")) {
                fieldMapping.setIdType(FieldMapping.IdType.Lead);
                fieldMapping.setMapToLatticeId(false);
                fieldMapping.setMappedToLatticeField(true);
            }
        }
        modelingFileMetadataService.resolveMetadata(mktSourceFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                mktLeadFeedType);
        mktSourceFile = sourceFileService.findByName(mktSourceFile.getName());

        String mktoLeadDFId = cdlService.createS3Template(customerSpace, mktSourceFile.getName(),
                SOURCE, ENTITY_CONTACT, mktLeadFeedType, DataFeedTask.SubType.Lead.name(), "LeadsData");
        Assert.assertNotNull(mktSourceFile);
        Assert.assertNotNull(mktoLeadDFId);
        return mktoLeadDFId;
    }

}
