package com.latticeengines.pls.end2end;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;

public class CSVFileImportAddLatticeFieldDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private static final String CONTACT_DATE_FILE = "Contact_Date.csv";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testAddLatticeField() {
        baseContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_DATE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_DATE_FILE));
        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_CONTACT);
        List<LatticeSchemaField> latticeSchema =
                modelingFileMetadataService.getSchemaToLatticeSchemaFields(ENTITY_CONTACT, SOURCE, feedType);
        boolean createdDate = false;
        for (LatticeSchemaField schemaField : latticeSchema) {
            if (schemaField.getName().equals("CreatedDate")) {
                createdDate = true;
                Assert.assertEquals(schemaField.getFieldType(), UserDefinedType.DATE);
                Assert.assertFalse(schemaField.getFromExistingTemplate());
            }
        }
        Assert.assertTrue(createdDate);

        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(baseContactFile.getName(), ENTITY_CONTACT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (StringUtils.isNotEmpty(fieldMapping.getMappedField())) {
                if (fieldMapping.getMappedField().equals("CreatedDate")) {
                    fieldMapping.setMappedField(null);
                }
            }
        }
        fieldMappingDocument.setIgnoredFields(Collections.singletonList("Created Date"));

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
        for (LatticeSchemaField schemaField : latticeSchema) {
            if (schemaField.getName().equals("CreatedDate")) {
                Assert.fail("Should not contains CreatedDate in template!");
            }
        }

        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(newContactFile.getName(), ENTITY_CONTACT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("Created Date")) {
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldMapping.getMappedField(), "CreatedDate");
            }
        }

        modelingFileMetadataService.resolveMetadata(newContactFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                feedType);
        newContactFile = sourceFileService.findByName(newContactFile.getName());

        dfIdExtra = cdlService.createS3Template(customerSpace, newContactFile.getName(),
                SOURCE, ENTITY_CONTACT, feedType, null, ENTITY_CONTACT + "Data");

        latticeSchema = modelingFileMetadataService.getSchemaToLatticeSchemaFields(ENTITY_CONTACT, SOURCE, feedType);
        createdDate = false;
        for (LatticeSchemaField schemaField : latticeSchema) {
            if (schemaField.getName().equals("CreatedDate")) {
                createdDate = true;
            }
        }
        Assert.assertTrue(createdDate);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, dfIdExtra);
        Assert.assertNotNull(dataFeedTask.getImportTemplate());
        Assert.assertNotNull(dataFeedTask.getImportTemplate().getAttribute(InterfaceName.CreatedDate));
    }
}
