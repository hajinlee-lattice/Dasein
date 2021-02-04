package com.latticeengines.pls.end2end2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionSectionName;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;

public class CSVFileImportDateFormatIW2DeploymentTestNG extends CSVFileImportIW2DeploymentTestNGBase {
    private static final String CONTACT_DATE_FILE = "Contact_Date.csv";

    private static final String CUSTOM = "Custom";
    private static final String STANDARD = "Standard";
    private static final String UNMAPPED = "unmapped";
    private RestTemplate restTemplate;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);
        flags.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), false);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG, flags);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        restTemplate = testBed.getRestTemplate();
    }

    @Test(groups = "deployment")
    public void testContactDate() throws Exception {
        baseContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Contacts.getSchemaInterpretation(), EntityType.Contacts.getEntity().name(),
                CONTACT_DATE_FILE, ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_DATE_FILE));
        String feedType = EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.Contacts);
        List<LatticeSchemaField> latticeSchema =
                modelingFileMetadataService.getSchemaToLatticeSchemaFields(EntityType.Contacts.getEntity().name(),
                        SOURCE, feedType);

        boolean createdDate = false;
        boolean lastModifiedDate = false;
        for (LatticeSchemaField schemaField : latticeSchema) {
            if (schemaField.getName().equals("CreatedDate")) {
                createdDate = true;
                Assert.assertEquals(schemaField.getFieldType(), UserDefinedType.DATE);
                Assert.assertFalse(schemaField.getFromExistingTemplate());
            }
            if (schemaField.getName().equals("LastModifiedDate")) {
                lastModifiedDate = true;
                Assert.assertEquals(schemaField.getFieldType(), UserDefinedType.DATE);
                Assert.assertFalse(schemaField.getFromExistingTemplate());
            }
        }
        Assert.assertTrue(createdDate);
        Assert.assertTrue(lastModifiedDate);
        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse = dataMappingService.fetchFieldDefinitions(
                DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE, EntityType.Contacts.getDisplayName(), baseContactFile.getName());

        FieldDefinitionsRecord fieldDefinitionsRecord =
                fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();

        for (FieldDefinition definition :
                fieldDefinitionsRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Analysis_Fields.getName())) {
            if ("Created Date".equals(definition.getColumnName())) {
                Assert.assertTrue(definition.isInCurrentImport());
                Assert.assertEquals(definition.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(definition.getDateFormat(), "MM/DD/YYYY");
            }
        }
        dataMappingService.commitFieldDefinitions(DEFAULT_SYSTEM,  DEFAULT_SYSTEM_TYPE,
                EntityType.Contacts.getDisplayName(), baseContactFile.getName(), false, fieldDefinitionsRecord);
        baseContactFile = sourceFileService.findByName(baseContactFile.getName());

        Assert.assertNotNull(baseContactFile);


        latticeSchema = modelingFileMetadataService.getSchemaToLatticeSchemaFields(EntityType.Contacts.getEntity().name(),
                SOURCE,
                feedType);
        createdDate = false;
        lastModifiedDate = false;
        for (LatticeSchemaField schemaField : latticeSchema) {
            if (schemaField.getName().equals("CreatedDate")) {
                createdDate = true;
                Assert.assertEquals(schemaField.getFieldType(), UserDefinedType.DATE);
                Assert.assertTrue(schemaField.getFromExistingTemplate());
            }
            if (schemaField.getName().equals("LastModifiedDate")) {
                lastModifiedDate = true;
            }
        }
        Assert.assertTrue(createdDate);
        Assert.assertFalse(lastModifiedDate);

        SourceFile newContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Contacts.getSchemaInterpretation(), EntityType.Contacts.getEntity().name(), CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        fetchFieldDefinitionsResponse = dataMappingService.fetchFieldDefinitions(DEFAULT_SYSTEM,
                DEFAULT_SYSTEM_TYPE, EntityType.Contacts.getDisplayName(), newContactFile.getName());
        fieldDefinitionsRecord =
                fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();

        for (FieldDefinition definition :
                fieldDefinitionsRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Analysis_Fields.getName())) {
            if ("Created Date".equals(definition.getColumnName())) {
                Assert.assertEquals(definition.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(definition.getDateFormat(), "MM/DD/YYYY");
            } else if ("LastModifiedDate".equals(definition.getColumnName())) {
                Assert.assertEquals(definition.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(definition.getTimeFormat(), "00:00:00 12H");
            }
        }


    }



    @Test(groups = "deployment")
    public void testAccountDateFormat() throws Exception {
        baseAccountFile = uploadSourceFile(DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE, EntityType.Accounts,
                ACCOUNT_SOURCE_FILE);
        String dateFormatString1 = "DD/MM/YYYY";
        String timezone1 = "America/New_York";
        String dateFormatString2 = "MM.DD.YY";
        String timeFormatString2 = "00:00:00 24H";
        String timezone2 = "Asia/Shanghai";
        String storedDateFormatString1 = "MM/DD/YYYY";
        String storedTimeFormatString2 = "00:00:00 12H";

        Assert.assertNotNull(baseAccountFile);
        String feedType = EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.Accounts);
        String dfId = cdlService.createS3Template(customerSpace, baseAccountFile.getName(),
                SOURCE, EntityType.Accounts.getEntity().name(), feedType, null, feedType);

        SourceFile accountDateSF = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Accounts.getSchemaInterpretation(), EntityType.Accounts.getEntity().name(),
                ACCOUNT_SOURCE_FILE_FROMATDATE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE_FROMATDATE));

        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse =
                dataMappingService.fetchFieldDefinitions(DEFAULT_SYSTEM,
                        DEFAULT_SYSTEM_TYPE, EntityType.Accounts.getDisplayName(), accountDateSF.getName());
        FieldDefinitionsRecord currentRecord = fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();
        String customFieldsSectionName = fetchFieldDefinitionsResponse.getImportWorkflowSpec()
                .provideCustomFieldsSectionName();
        for (FieldDefinition definition :
                currentRecord.getFieldDefinitionsRecords(customFieldsSectionName)) {
            if (definition.getColumnName().equals("TestDate1")) {
                Assert.assertEquals(definition.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(definition.getDateFormat(), dateFormatString1);
                // change the auto detection result for date pattern
                definition.setFieldType(UserDefinedType.DATE);
                definition.setDateFormat(storedDateFormatString1);
                definition.setTimeZone(timezone1);
            } else if (definition.getColumnName().equals("TestDate2")) {
                Assert.assertEquals(definition.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(definition.getDateFormat(), dateFormatString2);
                Assert.assertEquals(definition.getTimeFormat(), timeFormatString2);
                // change the auto detection result for time pattern
                definition.setFieldType(UserDefinedType.DATE);
                definition.setDateFormat(dateFormatString2);
                definition.setTimeFormat(storedTimeFormatString2);
                definition.setTimeZone(timezone2);
            }
        }
        dataMappingService.commitFieldDefinitions(DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE,
                EntityType.Accounts.getDisplayName(), accountDateSF.getName(), false, currentRecord);
        accountDateSF = sourceFileService.findByName(accountDateSF.getName());

        String dfIdExtra = cdlService.createS3Template(customerSpace, accountDateSF.getName(),
                SOURCE, EntityType.Accounts.getEntity().name(), feedType, null, feedType);

        fetchFieldDefinitionsResponse = dataMappingService.fetchFieldDefinitions(DEFAULT_SYSTEM,
                DEFAULT_SYSTEM_TYPE, EntityType.Accounts.getDisplayName(), accountDateSF.getName());
        currentRecord = fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();
        for (FieldDefinition fieldDefinition : currentRecord.getFieldDefinitionsRecords(customFieldsSectionName)) {
            if (fieldDefinition.getColumnName().equals("TestDate1")) {
                Assert.assertEquals(fieldDefinition.getDateFormat(), storedDateFormatString1);
                Assert.assertEquals(fieldDefinition.getTimeZone(), timezone1);
            } else if (fieldDefinition.getColumnName().equals("TestDate2")) {
                Assert.assertEquals(fieldDefinition.getDateFormat(), dateFormatString2);
                Assert.assertEquals(fieldDefinition.getTimeFormat(), storedTimeFormatString2);
                Assert.assertEquals(fieldDefinition.getTimeZone(), timezone2);
            }
        }

        Assert.assertEquals(dfId, dfIdExtra);
    }
}
