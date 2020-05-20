package com.latticeengines.pls.end2end2;

import java.util.List;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
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
                EntityType.Contacts.getSchemaInterpretation(), EntityType.Contacts.getEntity().name(), CONTACT_DATE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_DATE_FILE));
        String feedType = EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.Contacts);
        List<LatticeSchemaField> latticeSchema =
                modelingFileMetadataService.getSchemaToLatticeSchemaFields(EntityType.Contacts.getEntity().name(), SOURCE, feedType);
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
                dataMappingService.fetchFieldDefinitions(DEFAULT_SYSTEM,
                        DEFAULT_SYSTEM_TYPE, EntityType.Contacts.getDisplayName(), baseContactFile.getName());

        FieldDefinitionsRecord fieldDefinitionsRecord =
                fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();
        for (FieldDefinition definition :
                fieldDefinitionsRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Analysis_Fields.getName())) {
            if ("CreatedDate".equals(definition.getFieldName())) {
                definition.setIgnored(Boolean.TRUE);
            }
        }
        S3ImportSystem importSystem = cdlService.getS3ImportSystem(mainTestTenant.getName(), DEFAULT_SYSTEM);
        importSystem.setAccountSystemId(importSystem.generateAccountSystemId());
        cdlService.updateS3ImportSystem(mainTestTenant.getName(), importSystem);

        dataMappingService.commitFieldDefinitions(DEFAULT_SYSTEM,  DEFAULT_SYSTEM_TYPE,
                EntityType.Contacts.getDisplayName(), baseContactFile.getName(), false, fieldDefinitionsRecord);
        baseContactFile = sourceFileService.findByName(baseContactFile.getName());

        Assert.assertNotNull(baseContactFile);

        latticeSchema = modelingFileMetadataService.getSchemaToLatticeSchemaFields(EntityType.Contacts.getEntity().name(),
                SOURCE, feedType);
        for (LatticeSchemaField schemaField : latticeSchema) {
            if ("CreatedDate".equals(schemaField.getName())) {
                Assert.fail("Should not contains CreatedDate in template!");
            }
        }

        SourceFile newContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Contacts.getSchemaInterpretation(), EntityType.Contacts.getEntity().name(), CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        fetchFieldDefinitionsResponse =
                dataMappingService.fetchFieldDefinitions(DEFAULT_SYSTEM,
                        DEFAULT_SYSTEM_TYPE, EntityType.Contacts.getDisplayName(), newContactFile.getName());
        fieldDefinitionsRecord =
                fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();
        for (FieldDefinition fieldDefinition :
                fieldDefinitionsRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Analysis_Fields.getName())) {
            if ("Created Date".equals(fieldDefinition.getColumnName())) {
                Assert.assertEquals(fieldDefinition.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldDefinition.getFieldName(), "CreatedDate");
            }
        }

        dataMappingService.commitFieldDefinitions(DEFAULT_SYSTEM,  DEFAULT_SYSTEM_TYPE,
                EntityType.Contacts.getDisplayName(), newContactFile.getName(), false, fieldDefinitionsRecord);


        latticeSchema = modelingFileMetadataService.getSchemaToLatticeSchemaFields(EntityType.Contacts.getEntity().name(),
                SOURCE, feedType);
        createdDate = false;
        for (LatticeSchemaField schemaField : latticeSchema) {
            if (schemaField.getName().equals("CreatedDate")) {
                createdDate = true;
            }
        }
        Assert.assertTrue(createdDate);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.Contacts));
        Assert.assertNotNull(dataFeedTask.getImportTemplate());
        Assert.assertNotNull(dataFeedTask.getImportTemplate().getAttribute(InterfaceName.CreatedDate));
    }
}
