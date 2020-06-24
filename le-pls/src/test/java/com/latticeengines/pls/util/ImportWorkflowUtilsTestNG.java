package com.latticeengines.pls.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidationMessage;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.util.ImportWorkflowSpecUtils;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.proxy.exposed.core.ImportWorkflowSpecProxy;


public class ImportWorkflowUtilsTestNG extends PlsFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowUtilsTestNG.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ImportWorkflowSpecProxy importWorkflowSpecProxy;

    private String contactCsvHdfsPath = "/tmp/test_contact_import_workflow";
    private String dcpCsvHdfsPath = "/tmp/test_dcp_import_workflow";

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        String contactPath = ClassLoader
                .getSystemResource("com/latticeengines/pls/util/test-contact-import-file.csv").getPath();
        HdfsUtils.rmdir(yarnConfiguration, contactCsvHdfsPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, contactPath, contactCsvHdfsPath);

        String dcpPath = ClassLoader
                .getSystemResource("com/latticeengines/pls/util/test-dcp-beta-account.csv").getPath();
        HdfsUtils.rmdir(yarnConfiguration, dcpCsvHdfsPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dcpPath, dcpCsvHdfsPath);
    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, contactCsvHdfsPath);
        HdfsUtils.rmdir(yarnConfiguration, dcpCsvHdfsPath);
    }

    @Test(groups = "functional")
    public void testCreateFieldDefinitionsRecordFromSpecAndTable_noExistingTemplate() throws IOException {
        // Load CSV containing imported Contact data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(contactCsvHdfsPath, yarnConfiguration, null);

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/importfunctest-contacts-spec.json",
                ImportWorkflowSpec.class);

        // Generate actual FieldDefinitionsRecord based on Contact import CSV and Spec.
        FieldDefinitionsRecord actualRecord = ImportWorkflowUtils.createFieldDefinitionsRecordFromSpecAndTable(
                importWorkflowSpec, null, resolver);
        log.info("Actual (no existing template) fieldDefinitionsRecord is:\n" + JsonUtils.pprint(actualRecord));

        // Get expected FieldDefinitionRecord result from resource file.
        FieldDefinitionsRecord expectedRecord = JsonUtils.pojoFromJsonResourceFile(
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
        MetadataResolver resolver = new MetadataResolver(contactCsvHdfsPath, yarnConfiguration, null);

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/importfunctest-contacts-spec.json",
                ImportWorkflowSpec.class);

        log.info("Import Workflow Spec is:\n" + JsonUtils.pprint(importWorkflowSpec));

        // Generate Table containing existing template from resource file.
        Table existingTemplateTable = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-existing-contact-template.json", Table.class);

        log.info("Existing Table is:\n" + JsonUtils.pprint(existingTemplateTable));

        // Generate actual FieldDefinitionsRecord based on Contact import CSV, Spec, and existing template table.
        FieldDefinitionsRecord actualRecord = ImportWorkflowUtils.createFieldDefinitionsRecordFromSpecAndTable(
                importWorkflowSpec, existingTemplateTable, resolver);
        log.info("Actual (existing template) fieldDefinitionsRecord is:\n" + JsonUtils.pprint(actualRecord));

        // Get expected FieldDefinitionRecord result from resource file.
        FieldDefinitionsRecord expectedRecord = JsonUtils.pojoFromJsonResourceFile(
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
        // Initialize the actual FetchFieldDefinitionsResponse.
        FetchFieldDefinitionsResponse actualResponse = new FetchFieldDefinitionsResponse();
        actualResponse.setCurrentFieldDefinitionsRecord(
                new FieldDefinitionsRecord("ImportWorkflowUtilsTest", "ImportFuncTest", "Contacts"));

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/importfunctest-contacts-spec.json",
                ImportWorkflowSpec.class);
        log.info("Import Workflow Spec is:\n" + JsonUtils.pprint(importWorkflowSpec));
        actualResponse.setImportWorkflowSpec(importWorkflowSpec);

        // Load CSV containing imported Contact data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(contactCsvHdfsPath, yarnConfiguration, null);
        actualResponse.setAutodetectionResultsMap(ImportWorkflowUtils.generateAutodetectionResultsMap(resolver));

        // Generate actual FieldDefinitionsRecord based on Contact import CSV and Spec.
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(actualResponse);
        log.info("Actual (no existing template) fetchFieldDefinitionsResponse is:\n" + JsonUtils.pprint(actualResponse));

        // Get expected FetchFieldDefinitionsResponse result from resource file.
        FetchFieldDefinitionsResponse expectedResponse = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-response-contact-no-existing-template.json",
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
        // Initialize the actual FetchFieldDefinitionsResponse.
        FetchFieldDefinitionsResponse actualResponse = new FetchFieldDefinitionsResponse();
        actualResponse.setCurrentFieldDefinitionsRecord(
                new FieldDefinitionsRecord("ImportWorkflowUtilsTest", "ImportFuncTest", "Contacts"));

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/importfunctest-contacts-spec.json",
                ImportWorkflowSpec.class);
        log.info("Import Workflow Spec is:\n" + JsonUtils.pprint(importWorkflowSpec));
        actualResponse.setImportWorkflowSpec(importWorkflowSpec);

        // Generate Table containing existing template from resource file.
        Table existingTemplateTable = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-existing-contact-template.json", Table.class);
        log.info("Existing Table is:\n" + JsonUtils.pprint(existingTemplateTable));
        actualResponse.setExistingFieldDefinitionsMap(
                ImportWorkflowUtils.getFieldDefinitionsMapFromTable(existingTemplateTable));

        // Load CSV containing imported Contact data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(contactCsvHdfsPath, yarnConfiguration, null);
        actualResponse.setAutodetectionResultsMap(ImportWorkflowUtils.generateAutodetectionResultsMap(resolver));

        // Generate actual FieldDefinitionsRecord based on Contact import CSV and Spec.
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(actualResponse);
        log.info("Actual (existing template) fetchFieldDefinitionsResponse is:\n" + JsonUtils.pprint(actualResponse));

        // Get expected FetchFieldDefinitionsResponse result from resource file.
        FetchFieldDefinitionsResponse expectedResponse = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-response-contact-existing-template.json",
                FetchFieldDefinitionsResponse.class);
        log.info("Expected (existing template) fetchFieldDefinitionsResponse is:\n" + JsonUtils.pprint(expectedResponse));

        ObjectMapper mapper = new ObjectMapper();
        Assert.assertTrue(mapper.valueToTree(actualResponse).equals(mapper.valueToTree(expectedResponse)));

        Assert.assertEquals(actualResponse, expectedResponse,
                "Actual Response:\n" + JsonUtils.pprint(actualResponse) + "\nvs\n\nExpected Response:\n" +
                        JsonUtils.pprint(expectedResponse));
    }

    @Test(groups = "functional")
    public void testGenerateValidationResponse() throws IOException {
        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/importfunctest-contacts-spec.json",
                ImportWorkflowSpec.class);
        log.info("Import Workflow Spec is:\n" + JsonUtils.pprint(importWorkflowSpec));
        String customFieldsSectionName = importWorkflowSpec.provideCustomFieldsSectionName();

        // Load CSV containing imported Contact data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(contactCsvHdfsPath, yarnConfiguration, null);
        Map<String, FieldDefinition> autoDetectionResultsMap =
                ImportWorkflowUtils.generateAutodetectionResultsMap(resolver);

        // Initialize the FetchFieldDefinitionsResponse.
        FetchFieldDefinitionsResponse fetchResponse = new FetchFieldDefinitionsResponse();
        fetchResponse.setCurrentFieldDefinitionsRecord(
                new FieldDefinitionsRecord("ImportWorkflowUtilsTest", "ImportFuncTest", "Contacts"));
        fetchResponse.setAutodetectionResultsMap(autoDetectionResultsMap);
        fetchResponse.setImportWorkflowSpec(importWorkflowSpec);

        // Generate the current field definition to mimic "fetch".
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(fetchResponse);
        FieldDefinitionsRecord currentFieldDefinitionsRecord = fetchResponse.getCurrentFieldDefinitionsRecord();
        Map<String, List<FieldDefinition>> fieldDefinitionMap =
                currentFieldDefinitionsRecord.getFieldDefinitionsRecordsMap();

        // change customer field's type
        Map<String, FieldDefinition> customNameToFieldDefinition =
                fieldDefinitionMap.getOrDefault(customFieldsSectionName, new ArrayList<>()).stream()
                        .collect(Collectors.toMap(FieldDefinition::getColumnName, field -> field));
        FieldDefinition customField = customNameToFieldDefinition.get("Earnings");
        Assert.assertNotNull(customField);
        customField.setFieldType(UserDefinedType.TEXT);
        ValidateFieldDefinitionsResponse response = ImportWorkflowUtils.generateValidationResponse(fieldDefinitionMap,
                autoDetectionResultsMap, importWorkflowSpec, null,null, resolver);
        Assert.assertEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.WARNING);
        checkGeneratedResult(response, customFieldsSectionName, "Earnings", customFieldsSectionName,
                FieldValidationMessage.MessageLevel.WARNING,
                "column Earnings is set as TEXT but appears to only have NUMBER values");

        // change field type for lattice field
        Map<String, FieldDefinition> IDNameToFieldDefinition =
                fieldDefinitionMap.getOrDefault("Unique ID", new ArrayList<>()).stream()
                        .collect(Collectors.toMap(FieldDefinition::getFieldName, field -> field));
        FieldDefinition IDDefinition = IDNameToFieldDefinition.get("CustomerContactId");
        Assert.assertNotNull(IDDefinition);
        IDDefinition.setFieldType(UserDefinedType.INTEGER);
        response = ImportWorkflowUtils.generateValidationResponse(fieldDefinitionMap, autoDetectionResultsMap,
                        importWorkflowSpec, null,null,resolver);
        Assert.assertEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
        checkGeneratedResult(response, "Unique ID", "CustomerContactId", customFieldsSectionName,
                FieldValidationMessage.MessageLevel.ERROR,
                "the current template has fieldType INTEGER while the Spec has fieldType TEXT for field Contact ID");
    }

    @Test(groups = "functional")
    public void testIgnoredFlag() throws Exception {
        FetchFieldDefinitionsResponse actualResponse = new FetchFieldDefinitionsResponse();
        actualResponse.setCurrentFieldDefinitionsRecord(
                new FieldDefinitionsRecord("ImportWorkflowUtilsTest", "ImportFuncTest", "Contacts"));

        // Generate Spec Java class from resource file
        ImportWorkflowSpec importWorkflowSpec = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/importfunctest-contacts-spec.json", ImportWorkflowSpec.class);

        actualResponse.setImportWorkflowSpec(importWorkflowSpec);

        // load csv and generate auto-detected field definitions
        MetadataResolver resolver = new MetadataResolver(contactCsvHdfsPath, yarnConfiguration, null);
        Map<String, FieldDefinition> autodetectionResultsMap =
                ImportWorkflowUtils.generateAutodetectionResultsMap(resolver);

        actualResponse.setAutodetectionResultsMap(autodetectionResultsMap);
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(actualResponse);

        FieldDefinitionsRecord currentRecord = actualResponse.getCurrentFieldDefinitionsRecord();
        List<FieldDefinition> customFieldDefinitions = currentRecord.getFieldDefinitionsRecordsMap().getOrDefault(
                "Custom Fields", new ArrayList<>());

        Assert.assertNotNull(customFieldDefinitions);
        Map<String, FieldDefinition> customNameToFieldDefinition =
                customFieldDefinitions.stream().collect(Collectors.toMap(FieldDefinition::getColumnName,
                        field -> field));
        FieldDefinition earningsDefinition = customNameToFieldDefinition.get("Earnings");
        Assert.assertNotNull(earningsDefinition);
        earningsDefinition.setIgnored(Boolean.TRUE);
        Table result = ImportWorkflowSpecUtils.getTableFromFieldDefinitionsRecord(null, false, currentRecord);

        List<Attribute> attrs = result.getAttributes();
        Attribute earningsAttr =
                attrs.stream().filter(attr -> "Earnings".equals(attr.getDisplayName())).findFirst().orElse(null);

        Assert.assertNull(earningsAttr);
    }

    @Test(groups = "functional")
    public void testGenerateCurrentFieldDefinitionRecord_noExistingTemplate_DcpAccounts() throws IOException {
        // Initialize the actual FetchFieldDefinitionsResponse.
        FetchFieldDefinitionsResponse actualResponse = new FetchFieldDefinitionsResponse();
        actualResponse.setCurrentFieldDefinitionsRecord(
                new FieldDefinitionsRecord("ImportWorkflowUtilsTest", "ImportFuncDcpTest", "Accounts"));

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/importfuncdcptest-accounts-spec.json",
                ImportWorkflowSpec.class);
        log.info("Import Workflow Spec is:\n" + JsonUtils.pprint(importWorkflowSpec));
        actualResponse.setImportWorkflowSpec(importWorkflowSpec);

        // Load CSV containing imported DCP Accounts data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(dcpCsvHdfsPath, yarnConfiguration, null);
        actualResponse.setAutodetectionResultsMap(ImportWorkflowUtils.generateAutodetectionResultsMap(resolver));

        // Generate actual FieldDefinitionsRecord based on DCP Accounts import CSV and Spec.
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(actualResponse);
        log.info("Actual (no existing template) fetchFieldDefinitionsResponse is:\n" +
                JsonUtils.pprint(actualResponse));

        // Get expected FetchFieldDefinitionsResponse result from resource file.
        FetchFieldDefinitionsResponse expectedResponse = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-response-dcp-no-existing-template.json",
                FetchFieldDefinitionsResponse.class);
        log.info("Expected (no existing template) fetchFieldDefinitionsResponse is:\n" +
                JsonUtils.pprint(expectedResponse));

        ObjectMapper mapper = new ObjectMapper();
        Assert.assertTrue(mapper.valueToTree(actualResponse).equals(mapper.valueToTree(expectedResponse)));

        Assert.assertEquals(actualResponse, expectedResponse,
                "Actual Record:\n" + JsonUtils.pprint(actualResponse) + "\nvs\n\nExpected Record:\n" +
                        JsonUtils.pprint(expectedResponse));


        // JAW-DEBUG
        Table table = ImportWorkflowSpecUtils.getTableFromFieldDefinitionsRecord(null, false,
                actualResponse.getCurrentFieldDefinitionsRecord());
        log.info("Attributes created from fetchFieldDefinitionsResponse are:\n" +
                JsonUtils.pprint(table));
    }

    @Test(groups = "functional")
    public void testGenerateCurrentFieldDefinitionRecord_withExistingTemplate_DcpAccounts() throws IOException {
        // Initialize the actual FetchFieldDefinitionsResponse.
        FetchFieldDefinitionsResponse actualResponse = new FetchFieldDefinitionsResponse();
        actualResponse.setCurrentFieldDefinitionsRecord(
                new FieldDefinitionsRecord("ImportWorkflowUtilsTest", "ImportFuncTest", "Contacts"));

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/importfuncdcptest-accounts-spec.json",
                ImportWorkflowSpec.class);
        log.info("Import Workflow Spec is:\n" + JsonUtils.pprint(importWorkflowSpec));
        actualResponse.setImportWorkflowSpec(importWorkflowSpec);

        // Generate Table containing existing template from resource file.
        Table existingTemplateTable = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-existing-dcp-template.json", Table.class);
        log.info("Existing Table is:\n" + JsonUtils.pprint(existingTemplateTable));
        actualResponse.setExistingFieldDefinitionsMap(
                ImportWorkflowUtils.getFieldDefinitionsMapFromTable(existingTemplateTable));

        // Load CSV containing imported DCP Accounts data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(dcpCsvHdfsPath, yarnConfiguration, null);
        actualResponse.setAutodetectionResultsMap(ImportWorkflowUtils.generateAutodetectionResultsMap(resolver));

        // Generate actual FieldDefinitionsRecord based on DCP Accounts import CSV and Spec.
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(actualResponse);
        log.info("Actual (existing template) fetchFieldDefinitionsResponse is:\n" + JsonUtils.pprint(actualResponse));

        // Get expected FetchFieldDefinitionsResponse result from resource file.
        FetchFieldDefinitionsResponse expectedResponse = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-response-dcp-existing-template.json",
                FetchFieldDefinitionsResponse.class);
        log.info("Expected (existing template) fetchFieldDefinitionsResponse is:\n" +
                JsonUtils.pprint(expectedResponse));

        ObjectMapper mapper = new ObjectMapper();
        Assert.assertTrue(mapper.valueToTree(actualResponse).equals(mapper.valueToTree(expectedResponse)));

        Assert.assertEquals(actualResponse, expectedResponse,
                "Actual Response:\n" + JsonUtils.pprint(actualResponse) + "\nvs\n\nExpected Response:\n" +
                        JsonUtils.pprint(expectedResponse));
    }

    @Test(groups = "functional")
    public void testGenerateValidationResponse_DcpAccounts() throws IOException {
        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = JsonUtils.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/importfuncdcptest-accounts-spec.json",
                ImportWorkflowSpec.class);
        log.info("Import Workflow Spec is:\n" + JsonUtils.pprint(importWorkflowSpec));
        String customFieldsSectionName = importWorkflowSpec.provideCustomFieldsSectionName();

        // Load CSV containing imported DCP Accounts data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(dcpCsvHdfsPath, yarnConfiguration, null);
        Map<String, FieldDefinition> autoDetectionResultsMap = ImportWorkflowUtils.generateAutodetectionResultsMap(resolver);

        // Initialize the FetchFieldDefinitionsResponse.
        FetchFieldDefinitionsResponse fetchResponse = new FetchFieldDefinitionsResponse();
        fetchResponse.setCurrentFieldDefinitionsRecord(new FieldDefinitionsRecord("ImportWorkflowUtilsTest",
                "ImportFuncDcpTest", "Accounts"));
        fetchResponse.setImportWorkflowSpec(importWorkflowSpec);
        fetchResponse.setAutodetectionResultsMap(autoDetectionResultsMap);
        log.info("fetchFieldDefinitionsResponse is:\n" + JsonUtils.pprint(fetchResponse));

        // Generate the current field definition to mimic "fetch".
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(fetchResponse);
        FieldDefinitionsRecord currentFieldDefinitionsRecord = fetchResponse.getCurrentFieldDefinitionsRecord();
        Map<String, List<FieldDefinition>> fieldDefinitionMap =
                currentFieldDefinitionsRecord.getFieldDefinitionsRecordsMap();

        // Test validation by triggering some warnings and errors.
        // First set "additionalData" field "Annual Revenue" to TEXT type and validate the warning.
        Map<String, FieldDefinition> additionalColumnNameToFieldDefinitionMap =
                fieldDefinitionMap.getOrDefault(customFieldsSectionName, new ArrayList<>()).stream()
                        .collect(Collectors.toMap(FieldDefinition::getColumnName, field -> field));
        FieldDefinition annualRevenueField = additionalColumnNameToFieldDefinitionMap.get("Annual Revenue");
        Assert.assertNotNull(annualRevenueField);
        annualRevenueField.setFieldType(UserDefinedType.TEXT);
        ValidateFieldDefinitionsResponse response = ImportWorkflowUtils.generateValidationResponse(fieldDefinitionMap,
                autoDetectionResultsMap, importWorkflowSpec, null, null, resolver);
        log.info("Validate response for Annual Revenue is:\n" + response.toString());

        Assert.assertEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.WARNING);
        Assert.assertEquals(response.getFieldValidationMessages(customFieldsSectionName).size(), 1);
        checkGeneratedResult(response, customFieldsSectionName, "Annual Revenue", customFieldsSectionName,
                FieldValidationMessage.MessageLevel.WARNING,
                "column Annual Revenue is set as TEXT but appears to only have NUMBER values");


        // Next set "additionalData" field "Annual Revenue" to DATE type and validate warnings and errror.
        annualRevenueField.setFieldType(UserDefinedType.DATE);
        response = ImportWorkflowUtils.generateValidationResponse(fieldDefinitionMap,
                autoDetectionResultsMap, importWorkflowSpec, null, null, resolver);
        log.info("Validate response for Annual Revenue is:\n" + response.toString());

        Assert.assertEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
        Assert.assertEquals(response.getFieldValidationMessages(customFieldsSectionName).size(), 2);
        checkGeneratedResult(response, customFieldsSectionName, "Annual Revenue", customFieldsSectionName,
                FieldValidationMessage.MessageLevel.WARNING,
                "column Annual Revenue is set as DATE but appears to only have NUMBER values");

        checkGeneratedResult(response, customFieldsSectionName, "Annual Revenue", customFieldsSectionName,
                FieldValidationMessage.MessageLevel.ERROR,
                "Date Format shouldn't be empty for column Annual Revenue with date type");

        // Reset "Annual Revenue" field.
        annualRevenueField.setFieldType(UserDefinedType.NUMBER);
        annualRevenueField.setFieldType(UserDefinedType.NUMBER);

        // Next unmap the required "Country" field from "companyInformation" section and test the validation error.
        Map<String, FieldDefinition> companyFieldNameToFieldDefinitionMap =
                fieldDefinitionMap.getOrDefault("companyInformation", new ArrayList<>()).stream()
                        .collect(Collectors.toMap(FieldDefinition::getFieldName, field -> field));
        FieldDefinition countryDefinition = companyFieldNameToFieldDefinitionMap.get("Country");
        Assert.assertNotNull(countryDefinition);
        countryDefinition.setColumnName(null);
        countryDefinition.setInCurrentImport(false);

        response = ImportWorkflowUtils.generateValidationResponse(fieldDefinitionMap, autoDetectionResultsMap,
                importWorkflowSpec, null, null, resolver);
        Assert.assertEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
        Assert.assertEquals(response.getFieldValidationMessages("companyInformation").size(), 1);
        checkGeneratedResult(response, "companyInformation", "Country", customFieldsSectionName,
                FieldValidationMessage.MessageLevel.ERROR,
                "Field name Country is required, needs set column name");

        log.info("Validate response for Country is:\n" + response.toString());
    }

    public static void checkGeneratedResult(ValidateFieldDefinitionsResponse response, String section, String name,
                                            String customFieldsSectionName,
                                            FieldValidationMessage.MessageLevel messageLevel, String errMSG) {
        Map<String, List<FieldValidationMessage>> validationMap = response.getFieldValidationMessagesMap();
        Assert.assertNotNull(validationMap);
        List<FieldValidationMessage> validationMessages = validationMap.get(section);
        Assert.assertNotNull(validationMessages);
        if (customFieldsSectionName.equals(section)) {
            FieldValidationMessage validation =
                    validationMessages.stream().filter(message -> StringUtils.equals(name, message.getColumnName())
                            && messageLevel.equals(message.getMessageLevel())
                            && errMSG.equals(message.getMessage())).findFirst().orElse(null);
            if (validation == null) {
                log.error("ValidationResponse without validation is:\n" + JsonUtils.pprint(response));
            }
            Assert.assertNotNull(validation);
        } else {
            FieldValidationMessage validation =
                    validationMessages.stream().filter(message -> StringUtils.equals(name, message.getFieldName())
                            && messageLevel.equals(message.getMessageLevel())
                            && errMSG.equals(message.getMessage())).findFirst().orElse(null);
            if (validation == null) {
                log.error("ValidationResponse without validation is:\n" + JsonUtils.pprint(response));
            }
            Assert.assertNotNull(validation);
        }
    }
}
