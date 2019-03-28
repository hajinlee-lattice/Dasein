package com.latticeengines.pls.end2end;

import java.util.List;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;

public class CSVFileImportDateFormatDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private static final String CONTACT_DATE_FILE = "Contact_Date.csv";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testContactDate() {
        baseContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_DATE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_DATE_FILE));
        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_CONTACT);
        List<LatticeSchemaField> latticeSchema =
                modelingFileMetadataService.getSchemaToLatticeSchemaFields(ENTITY_CONTACT, SOURCE, feedType);

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
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(baseContactFile.getName(), ENTITY_CONTACT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("Created Date")) {
                Assert.assertTrue(fieldMapping.isMappedToLatticeField());
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldMapping.isMappedToDateBefore(), false);
                Assert.assertEquals(fieldMapping.getDateFormatString(), "MM/DD/YYYY");
            }
        }
        modelingFileMetadataService.resolveMetadata(baseContactFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                feedType);
        baseContactFile = sourceFileService.findByName(baseContactFile.getName());

        String dfIdExtra = cdlService.createS3Template(customerSpace, baseContactFile.getName(),
                SOURCE, ENTITY_CONTACT, feedType, null, ENTITY_CONTACT + "Data");
        Assert.assertNotNull(baseContactFile);
        Assert.assertNotNull(dfIdExtra);

        SourceFile newContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        latticeSchema = modelingFileMetadataService.getSchemaToLatticeSchemaFields(ENTITY_CONTACT, SOURCE, feedType);
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

        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(newContactFile.getName(), ENTITY_CONTACT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("Created Date")) {
                Assert.assertTrue(fieldMapping.isMappedToLatticeField());
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldMapping.isMappedToDateBefore(), true);
                Assert.assertEquals(fieldMapping.getDateFormatString(), "MM/DD/YYYY");
            } else if (fieldMapping.getUserField().equals("LastModifiedDate")) {
                Assert.assertFalse(fieldMapping.isMappedToLatticeField());
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldMapping.isMappedToDateBefore(), false);
                Assert.assertEquals(fieldMapping.getTimeFormatString(), "00:00:00 12H");
            }
        }

    }

    @Test(groups = "deployment")
    public void testDateFormat() {
        baseAccountFile = uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
        String dateFormatString1 = "DD/MM/YYYY";
        String timezone1 = "America/New_York";
        String dateFormatString2 = "MM.DD.YY";
        String timeFormatString2 = "00:00:00 24H";
        String timezone2 = "Asia/Shanghai";
        String storedDateFormatString1 = "MM/DD/YYYY";
        String storedTimeFormatString2 = "00:00:00 12H";

        Assert.assertNotNull(baseAccountFile);

        String dfId = cdlService.createS3Template(customerSpace, baseAccountFile.getName(),
                SOURCE, ENTITY_ACCOUNT, getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT), null, ENTITY_ACCOUNT +
                        "Data");

        SourceFile accountDateSF = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE_FROMATDATE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE_FROMATDATE));

        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(accountDateSF.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("TestDate1")) {
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldMapping.getDateFormatString(), dateFormatString1);
                Assert.assertEquals(fieldMapping.isMappedToDateBefore(), false);
                // change the auto detection result for date pattern
                fieldMapping.setFieldType(UserDefinedType.DATE);
                fieldMapping.setMappedToLatticeField(false);
                fieldMapping.setDateFormatString(storedDateFormatString1);
                fieldMapping.setTimezone(timezone1);
            } else if (fieldMapping.getUserField().equals("TestDate2")) {
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldMapping.isMappedToDateBefore(), false);
                Assert.assertEquals(fieldMapping.getDateFormatString(), dateFormatString2);
                Assert.assertEquals(fieldMapping.getTimeFormatString(), timeFormatString2);
                // change the auto detection result for time pattern
                fieldMapping.setFieldType(UserDefinedType.DATE);
                fieldMapping.setMappedToLatticeField(false);
                fieldMapping.setDateFormatString(dateFormatString2);
                fieldMapping.setTimeFormatString(storedTimeFormatString2);
                fieldMapping.setTimezone(timezone2);
            }
        }

        modelingFileMetadataService.resolveMetadata(accountDateSF.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                feedType);
        accountDateSF = sourceFileService.findByName(accountDateSF.getName());

        String dfIdExtra = cdlService.createS3Template(customerSpace, accountDateSF.getName(),
                SOURCE, ENTITY_ACCOUNT, feedType, null, ENTITY_ACCOUNT + "Data");

        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(accountDateSF.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("TestDate1")) {
                Assert.assertEquals(fieldMapping.isMappedToDateBefore(), true);
                Assert.assertEquals(fieldMapping.getDateFormatString(), storedDateFormatString1);
                Assert.assertEquals(fieldMapping.getTimezone(), timezone1);
            } else if (fieldMapping.getUserField().equals("TestDate2")) {
                Assert.assertEquals(fieldMapping.isMappedToDateBefore(), true);
                Assert.assertEquals(fieldMapping.getDateFormatString(), dateFormatString2);
                Assert.assertEquals(fieldMapping.getTimeFormatString(), storedTimeFormatString2);
                Assert.assertEquals(fieldMapping.getTimezone(), timezone2);
            }
        }

        Assert.assertEquals(dfId, dfIdExtra);
    }
}
