package com.latticeengines.pls.end2end2;

import java.util.List;

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
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionSectionName;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;

public class CSVFileImportAddLatticeFieldDeploymentTestNGV2 extends CSVFileImportDeploymentTestNGBaseV2 {

    private static final String CONTACT_DATE_FILE = "Contact_Date.csv";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testAddLatticeField() throws Exception {
        baseContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_DATE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_DATE_FILE));
        String feedType = EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.Contacts);
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

        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse =
                modelingFileMetadataService.fetchFieldDefinitions(DEFAULT_SYSTEM,
                DEFAULT_SYSTEM_TYPE, EntityType.Contacts.getDisplayName(), baseContactFile.getName());

        FieldDefinitionsRecord fieldDefinitionsRecord =
                fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();
        for (FieldDefinition definition :
                fieldDefinitionsRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Analysis_Fields.getName())) {
            if ("CreatedDate".equals(definition.getFieldName())) {
                definition.setFieldName(null);
            }
        }
        //fieldMappingDocument.setIgnoredFields(Collections.singletonList("Created Date"));
        modelingFileMetadataService.commitFieldDefinitions(DEFAULT_SYSTEM,  DEFAULT_SYSTEM_TYPE,
                EntityType.Contacts.getDisplayName(), baseContactFile.getName(), false, fieldDefinitionsRecord);
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
            if ("CreatedDate".equals(schemaField.getName())) {
                Assert.fail("Should not contains CreatedDate in template!");
            }
        }
        fetchFieldDefinitionsResponse =
                modelingFileMetadataService.fetchFieldDefinitions(DEFAULT_SYSTEM,
                        DEFAULT_SYSTEM_TYPE, EntityType.Contacts.getDisplayName(), newContactFile.getName());
        fieldDefinitionsRecord =
                fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();
        for (FieldDefinition fieldDefinition :
                fieldDefinitionsRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Analysis_Fields.getName())) {
            if (fieldDefinition.getColumnName().equals("Created Date")) {
                Assert.assertEquals(fieldDefinition.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldDefinition.getFieldName(), "CreatedDate");
            }
        }

        modelingFileMetadataService.commitFieldDefinitions(DEFAULT_SYSTEM,  DEFAULT_SYSTEM_TYPE,
                EntityType.Contacts.getDisplayName(), newContactFile.getName(), false, fieldDefinitionsRecord);
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
