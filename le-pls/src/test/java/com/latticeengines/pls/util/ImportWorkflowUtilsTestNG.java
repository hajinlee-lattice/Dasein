package com.latticeengines.pls.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidationMessage;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;

public class ImportWorkflowUtilsTestNG extends PlsFunctionalTestNGBase {
    private static Logger log = LoggerFactory.getLogger(ImportWorkflowUtilsTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    String csvHdfsPath = "/tmp/test_import_workflow";

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        String path = ClassLoader
                .getSystemResource("com/latticeengines/pls/util/test-contact-import-file.csv").getPath();

        HdfsUtils.rmdir(yarnConfiguration, csvHdfsPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, path, csvHdfsPath);
    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, csvHdfsPath);
    }


    @Test(groups = "functional")
    public void testCreateFieldDefinitionsRecordFromSpecAndTable_noExistingTemplate() throws IOException {
        // Load CSV containing imported Contact data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(csvHdfsPath, yarnConfiguration, null);

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-contact-spec.json",
                ImportWorkflowSpec.class);

        // Generate actual FieldDefinitionsRecord based on Contact import CSV and Spec.
        FieldDefinitionsRecord actualRecord = ImportWorkflowUtils.createFieldDefinitionsRecordFromSpecAndTable(
                importWorkflowSpec, null, resolver);

        log.info("Actual (no existing template) fieldDefinitionsRecord is:\n" + JsonUtils.pprint(actualRecord));

        FieldDefinitionsRecord expectedRecord = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-record-no-existing-template.json",
                FieldDefinitionsRecord.class);

        log.info("Expected (no existing template) fieldDefinitionsRecord is:\n" + JsonUtils.pprint(expectedRecord));

        ObjectMapper mapper = new ObjectMapper();
        Assert.assertTrue(mapper.valueToTree(actualRecord).equals(mapper.valueToTree(expectedRecord)));

        Assert.assertEquals(actualRecord, expectedRecord,
                "Actual Record:\n" + JsonUtils.pprint(actualRecord) + "\nvs\n\nExpected Record:\n" +
                        JsonUtils.pprint(expectedRecord));
    }

    @Test(groups = "functional")
    public void testCreateFieldDefinitionsRecordFromSpecAndTable_withExistingTemplate() throws IOException {
        // Load CSV containing imported Contact data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(csvHdfsPath, yarnConfiguration, null);

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-contact-spec.json",
                ImportWorkflowSpec.class);

        log.error("Import Workflow Spec is:\n" + JsonUtils.pprint(importWorkflowSpec));

        // Generate Table containing existing template from resource file.
        Table existingTemplateTable = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-existing-contact-template.json", Table.class);

        log.error("Existing Table is:\n" + JsonUtils.pprint(existingTemplateTable));

        // Generate actual FieldDefinitionsRecord based on Contact import CSV, Spec, and existing template table.
        FieldDefinitionsRecord actualRecord = ImportWorkflowUtils.createFieldDefinitionsRecordFromSpecAndTable(
                importWorkflowSpec, existingTemplateTable, resolver);

        log.info("Actual (existing template) fieldDefinitionsRecord is:\n" + JsonUtils.pprint(actualRecord));

        FieldDefinitionsRecord expectedRecord = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-record-existing-template.json",
                FieldDefinitionsRecord.class);

        log.info("Expected (existing template) fieldDefinitionsRecord is:\n" + JsonUtils.pprint(expectedRecord));

        ObjectMapper mapper = new ObjectMapper();
        Assert.assertTrue(mapper.valueToTree(actualRecord).equals(mapper.valueToTree(expectedRecord)));

        Assert.assertEquals(actualRecord, expectedRecord,
                "Actual Record:\n" + JsonUtils.pprint(actualRecord) + "\nvs\n\nExpected Record:\n" +
                        JsonUtils.pprint(expectedRecord));
    }

    @Test(groups = "functional")
    public void testGenerateCurrentFieldDefinitionRecord_noExistingTemplate() throws IOException {
        FetchFieldDefinitionsResponse actualResponse = new FetchFieldDefinitionsResponse();

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-contact-spec.json",
                ImportWorkflowSpec.class);
        log.error("Import Workflow Spec is:\n" + JsonUtils.pprint(importWorkflowSpec));
        actualResponse.setImportWorkflowSpec(importWorkflowSpec);

        // Load CSV containing imported Contact data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(csvHdfsPath, yarnConfiguration, null);
        actualResponse.setAutodetectionResultsMap(ImportWorkflowUtils.generateAutodetectionResultsMap(resolver));

        // Generate actual FieldDefinitionsRecord based on Contact import CSV and Spec.
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(actualResponse);

        log.info("Actual (no existing template) fetchFieldDefinitionsResponse is:\n" + JsonUtils.pprint(actualResponse));

        FetchFieldDefinitionsResponse expectedResponse = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-response-no-existing-template.json",
                FetchFieldDefinitionsResponse.class);

        log.info("Expected (no existing template) fetchFieldDefinitionsResponse is:\n" +
                JsonUtils.pprint(expectedResponse));

        ObjectMapper mapper = new ObjectMapper();
        Assert.assertTrue(mapper.valueToTree(actualResponse).equals(mapper.valueToTree(expectedResponse)));

        Assert.assertEquals(actualResponse, expectedResponse,
                "Actual Record:\n" + JsonUtils.pprint(actualResponse) + "\nvs\n\nExpected Record:\n" +
                        JsonUtils.pprint(expectedResponse));
    }

    @Test(groups = "functional")
    public void testGenerateCurrentFieldDefinitionRecord_withExistingTemplate() throws IOException {
        FetchFieldDefinitionsResponse actualResponse = new FetchFieldDefinitionsResponse();

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-contact-spec.json",
                ImportWorkflowSpec.class);
        log.error("Import Workflow Spec is:\n" + JsonUtils.pprint(importWorkflowSpec));
        actualResponse.setImportWorkflowSpec(importWorkflowSpec);

        // Generate Table containing existing template from resource file.
        Table existingTemplateTable = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-existing-contact-template.json", Table.class);
        log.error("Existing Table is:\n" + JsonUtils.pprint(existingTemplateTable));
        actualResponse.setExistingFieldDefinitionsMap(
                ImportWorkflowUtils.getFieldDefinitionsMapFromTable(existingTemplateTable));

        // Load CSV containing imported Contact data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(csvHdfsPath, yarnConfiguration, null);
        actualResponse.setAutodetectionResultsMap(ImportWorkflowUtils.generateAutodetectionResultsMap(resolver));

        // Generate actual FieldDefinitionsRecord based on Contact import CSV and Spec.
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(actualResponse);

        log.info("Actual (existing template) fetchFieldDefinitionsResponse is:\n" + JsonUtils.pprint(actualResponse));

        FetchFieldDefinitionsResponse expectedResponse = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-response-existing-template.json",
                FetchFieldDefinitionsResponse.class);

        log.info("Expected (existing template) fetchFieldDefinitionsResponse is:\n" + JsonUtils.pprint(expectedResponse));

        ObjectMapper mapper = new ObjectMapper();
        Assert.assertTrue(mapper.valueToTree(actualResponse).equals(mapper.valueToTree(expectedResponse)));

        Assert.assertEquals(actualResponse, expectedResponse,
                "Actual Response:\n" + JsonUtils.pprint(actualResponse) + "\nvs\n\nExpected Response:\n" +
                        JsonUtils.pprint(expectedResponse));
    }

    @Test(groups = "functional")
    public void testGenerateValidation() throws IOException {

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-contact-spec.json",
                ImportWorkflowSpec.class);
        log.error("Import Workflow Spec is:\n" + JsonUtils.pprint(importWorkflowSpec));

        // Load CSV containing imported Contact data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(csvHdfsPath, yarnConfiguration, null);
        Map<String, FieldDefinition> autoDetectionResultsMap = ImportWorkflowUtils.generateAutodetectionResultsMap(resolver);


        FetchFieldDefinitionsResponse fetchResponse = new FetchFieldDefinitionsResponse();
        fetchResponse.setAutodetectionResultsMap(autoDetectionResultsMap);
        fetchResponse.setImportWorkflowSpec(importWorkflowSpec);
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(fetchResponse);
        FieldDefinitionsRecord currentFieldDefinitionsRecord = fetchResponse.getCurrentFieldDefinitionsRecord();
        Map<String, List<FieldDefinition>> fieldDefinitionMap =
                currentFieldDefinitionsRecord.getFieldDefinitionsRecordsMap();


        // change customer field's type
        Map<String, FieldDefinition> customNameToFieldDefinition =
                fieldDefinitionMap.getOrDefault(ImportWorkflowUtils.CUSTOM_FIELDS, new ArrayList<>()).stream().collect(Collectors.toMap(FieldDefinition::getColumnName, field -> field));
        FieldDefinition customField = customNameToFieldDefinition.get("Earnings");
        Assert.assertNotNull(customField);
        customField.setFieldType(UserDefinedType.TEXT);
        ValidateFieldDefinitionsResponse response = ImportWorkflowUtils.generateValidationResponse(fieldDefinitionMap
                , autoDetectionResultsMap, importWorkflowSpec.getFieldDefinitionsRecordsMap(), resolver);
        Assert.assertEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.WARNING);
        checkGeneratedResult(response, ImportWorkflowUtils.CUSTOM_FIELDS, "Earnings", FieldValidationMessage.MessageLevel.WARNING);

        // change field type for lattice field
        Map<String, FieldDefinition> IDNameToFieldDefinition =
                fieldDefinitionMap.getOrDefault("Unique ID", new ArrayList<>()).stream().collect(Collectors.toMap(FieldDefinition::getFieldName, field -> field));
        FieldDefinition IDDefinition = IDNameToFieldDefinition.get("CustomerContactId");
        Assert.assertNotNull(IDDefinition);
        IDDefinition.setFieldType(UserDefinedType.INTEGER);
        response = ImportWorkflowUtils.generateValidationResponse(fieldDefinitionMap, autoDetectionResultsMap,
                        importWorkflowSpec.getFieldDefinitionsRecordsMap(), resolver);
        Assert.assertEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
        checkGeneratedResult(response, "Unique ID", "CustomerContactId", FieldValidationMessage.MessageLevel.ERROR);

    }

    private void checkGeneratedResult(ValidateFieldDefinitionsResponse response, String section, String name,
                                      FieldValidationMessage.MessageLevel messageLevel) {
        Map<String, List<FieldValidationMessage>> validationMap = response.getFieldValidationMessagesMap();
        Assert.assertNotNull(validationMap);
        List<FieldValidationMessage> validationMessages = validationMap.get(section);
        Assert.assertNotNull(validationMessages);
        if (ImportWorkflowUtils.CUSTOM_FIELDS.equals(section)) {
            FieldValidationMessage validation =
                    validationMessages.stream().filter(message -> name.equals(message.getColumnName())).findFirst().orElse(null);
            Assert.assertNotNull(validation);
            Assert.assertEquals(validation.getMessageLevel(), messageLevel);
        } else {
            FieldValidationMessage validation =
                    validationMessages.stream().filter(message -> name.equals(message.getFieldName())).findFirst().orElse(null);
            Assert.assertNotNull(validation);
            Assert.assertEquals(validation.getMessageLevel(), messageLevel);
        }


    }

    // resourceJsonFileRelativePath should start "com/latticeengines/...".
    public static <T> T pojoFromJsonResourceFile(String resourceJsonFileRelativePath, Class<T> clazz) throws
            IOException {
        T pojo = null;
        try {
            InputStream jsonInputStream = ClassLoader.getSystemResourceAsStream(resourceJsonFileRelativePath);
            if (jsonInputStream == null) {
                throw new IOException("Failed to convert resource file " + resourceJsonFileRelativePath +
                        " to InputStream.  Please check path");
            }
            pojo = JsonUtils.deserialize(jsonInputStream, clazz);
            if (pojo == null) {
                String jsonString = IOUtils.toString(jsonInputStream, Charset.defaultCharset());
                throw new IOException("POJO was null. Failed to deserialize InputStream containing string: " +
                        jsonString);
            }
        } catch (IOException e1) {
            log.error("File to POJO conversion failed for resource file " + resourceJsonFileRelativePath +
                    " with error: ", e1);
            throw e1;
        } catch (IllegalStateException e2) {
            log.error("File to POJO conversion failed for resource file " + resourceJsonFileRelativePath +
                    " with error: ", e2);
            throw e2;
        }
        return pojo;
    }


}
