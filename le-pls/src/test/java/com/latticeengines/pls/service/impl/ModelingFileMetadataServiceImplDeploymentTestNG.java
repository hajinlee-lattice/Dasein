package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionSectionName;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidationMessage;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.util.ImportWorkflowUtilsTestNG;

public class ModelingFileMetadataServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Inject
    private ModelingFileMetadataService modelingFileMetadataService;

    private String localPath = "com/latticeengines/pls/util/";
    private String csvFileName = "test-contact-import-file.csv";

    private BusinessEntity entity = BusinessEntity.Contact;
    private ValidateFieldDefinitionsRequest validateRequest = new ValidateFieldDefinitionsRequest();
    private String fileName;

    @Inject
    private FileUploadService fileUploadService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);

        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(entity.name()), entity.name(), csvFileName,
                ClassLoader.getSystemResourceAsStream(localPath + csvFileName));

        fileName = sourceFile.getName();
        FetchFieldDefinitionsResponse  fetchResponse =  modelingFileMetadataService.fetchFieldDefinitions("Default",
                "Test", "Contacts", fileName);
        setValidateRequestFromFetchResponse(fetchResponse);
    }

    /**
    # ERROR if required field is missing from set of mapped Lattice Fields in the template (either in the original template or newly added during the import).
    # ERROR if multiple columnNames map to the same Spec (Lattice) Field.
    # ERROR if fieldType of Lattice Field in new template doesn’t match the Spec.
    # WARNING: If user does not map ColumnName that matches a Lattice Field to the corresponding Lattice Field.
    # WARNING if fieldType of Custom Field set by user doesn’t match the autodetected fieldType.
    # WARNING if the autodetected fieldType based on column data doesn’t match the User defined fieldType of a Lattice Field.
    Date Formats (For both Lattice and Custom Fields):
    # WARNING if data format selected by user can’t parse 10% or more of the date column rows and doesn’t match the autodetected date format.
    # WARNING if data format selected by user can’t parse 10% or more of the date columns’ rows even though it is the same as the autodetected date format
    Possible because autodetected format uses prior format in second round import if previous set.
    # WARNING if the data format selected by the user changed, even if it can parse more than 10% of the date columns’ rows.
    # WARNING if time zone is ISO 8601 but column values are not or time zone is not ISO 8601 but column values are.
     */
    @Test(groups = "deployment")
    public void testFieldDefinitionValidate_noExistingTemplate() throws Exception {
        FieldDefinitionsRecord currentFieldDefinitionRecord = validateRequest.getCurrentFieldDefinitionsRecord();

        Map<String, List<FieldDefinition>> fieldDefinitionMap =
                currentFieldDefinitionRecord.getFieldDefinitionsRecordsMap();

        Map<String, FieldDefinition> customNameToCustomFieldDefinition =
                fieldDefinitionMap.getOrDefault(FieldDefinitionSectionName.Custom_Fields.getName(),
                        new ArrayList<>()).stream()
                        .collect(Collectors.toMap(FieldDefinition::getColumnName, field -> field));
        Map<String, FieldDefinition> fieldNameToContactFieldDefinition =
                fieldDefinitionMap.getOrDefault(FieldDefinitionSectionName.Contact_Fields.getName(), new ArrayList<>()).stream()
                        .collect(Collectors.toMap(FieldDefinition::getFieldName, field -> field));
        Map<String, FieldDefinition> fieldNameToAnalysisFieldDefinition =
                fieldDefinitionMap.getOrDefault(FieldDefinitionSectionName.Analysis_Fields.getName(), new ArrayList<>()).stream()
                        .collect(Collectors.toMap(FieldDefinition::getFieldName, field -> field));

        // case 1: required flag change, error
        FieldDefinition nameDefinition = fieldNameToContactFieldDefinition.get("FirstName");
        Assert.assertNotNull(nameDefinition);
        nameDefinition.setRequired(Boolean.FALSE);
        ValidateFieldDefinitionsResponse validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "Default", "Test", "Contacts", fileName, validateRequest);

        System.out.println(JsonUtils.pprint(validateResponse));
        Assert.assertNotNull(validateResponse);
        Assert.assertEquals(validateResponse.getValidationResult(),
                ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), InterfaceName.FirstName.name(),
                FieldValidationMessage.MessageLevel.ERROR);

        // case 2: required field missing, error
        fieldNameToContactFieldDefinition.remove(InterfaceName.FirstName.name());
        fieldDefinitionMap.put(FieldDefinitionSectionName.Contact_Fields.getName(), new ArrayList<>(fieldNameToContactFieldDefinition.values()));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "Default", "Test", "Contacts", fileName, validateRequest);
        Assert.assertEquals(validateResponse.getValidationResult(),
                ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), InterfaceName.FirstName.name(), FieldValidationMessage.MessageLevel.ERROR);

        // case 3: multiple custom name map to same lattice fields
        // Last Name -> Last Name, LastName -> Last Name, in section "Contact Fields", custom filed "LastName" was
        // mapped lattice filed "Last Name", manually mapped "Last Name" to lattice field "Last Name"
        FieldDefinition lastNameDefinition = customNameToCustomFieldDefinition.get("Last Name");
        Assert.assertNotNull(lastNameDefinition);
        lastNameDefinition.setFieldName(InterfaceName.LastName.name());
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "Default", "Test", "Contacts", fileName, validateRequest);
        Assert.assertEquals(validateResponse.getValidationResult(),
                ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Contact_Fields.getName(),
                InterfaceName.LastName.name(),
                FieldValidationMessage.MessageLevel.ERROR);

        // case 4: change field type of Email from Text to Integer
        FieldDefinition emailDefinition = fieldNameToContactFieldDefinition.get(InterfaceName.Email.name());
        Assert.assertNotNull(emailDefinition);
        Assert.assertEquals(emailDefinition.getFieldType(), UserDefinedType.TEXT);
        emailDefinition.setFieldType(UserDefinedType.INTEGER);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "Default", "Test", "Contacts", fileName, validateRequest);
        Assert.assertEquals(validateResponse.getValidationResult(),
                ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), InterfaceName.Email.name(),
                FieldValidationMessage.MessageLevel.ERROR);

        // case 5: unmap the column name(Last Name in "Contact Fields") that match lattice field
        FieldDefinition lastNameInContactField = fieldNameToContactFieldDefinition.get(InterfaceName.LastName.name());
        lastNameInContactField.setColumnName(null);
        lastNameInContactField.setInCurrentImport(false);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions("Default", "Test", "Contacts",
                fileName, validateRequest);
        System.out.println(JsonUtils.pprint(validateResponse));
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Contact_Fields.getName(),
                InterfaceName.LastName.name(),
                FieldValidationMessage.MessageLevel.WARNING);

        // case 6: WARNING if fieldType of Custom Field set by user doesn’t match the autodetected fieldType.
        FieldDefinition earningDefinition = customNameToCustomFieldDefinition.get("Earnings");
        Assert.assertNotNull(earningDefinition);
        // change field type from auto-detected number to text
        earningDefinition.setFieldType(UserDefinedType.TEXT);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions("Default", "Text", "Contacts",
                fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Custom_Fields.getName(),
                "Earnings",
                FieldValidationMessage.MessageLevel.WARNING);

        // case 7: date format is not set when type is Date
        FieldDefinition createdDateDefinition =
                fieldNameToAnalysisFieldDefinition.get(InterfaceName.CreatedDate.name());
        Assert.assertNotNull(createdDateDefinition);
        createdDateDefinition.setDateFormat(null);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions("Default", "Text", "Contacts",
                fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Analysis_Fields.getName(),
                InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.ERROR);


    }


    @Test(groups = "deployment", dependsOnMethods = "testFieldDefinitionValidate_noExistingTemplate")
    public void testFieldDefinitionValidate_withExistingTemplate() throws Exception {
        System.out.println(JsonUtils.pprint(validateRequest));
        FieldDefinitionsRecord currentFieldDefinitionRecord = validateRequest.getCurrentFieldDefinitionsRecord();
        FieldDefinitionsRecord commitRecord = modelingFileMetadataService.commitFieldDefinitions("Default", "Test",
                "Contacts", fileName,
                false, currentFieldDefinitionRecord);
        FetchFieldDefinitionsResponse  fetchResponse =  modelingFileMetadataService.fetchFieldDefinitions("Default",
                "Test", "Contacts", fileName);
        setValidateRequestFromFetchResponse(fetchResponse);
        Assert.assertNotNull(commitRecord);


        // second round after the fetch api
        currentFieldDefinitionRecord = validateRequest.getCurrentFieldDefinitionsRecord();
        Map<String, List<FieldDefinition>> fieldDefinitionMap =
                currentFieldDefinitionRecord.getFieldDefinitionsRecordsMap();
        Map<String, FieldDefinition> customNameToCustomFieldDefinition =
                fieldDefinitionMap.getOrDefault(FieldDefinitionSectionName.Custom_Fields.getName(), new ArrayList<>()).stream()
                        .collect(Collectors.toMap(FieldDefinition::getColumnName, field -> field));
        Map<String, FieldDefinition> fieldNameToAnalysisFieldDefinition =
                fieldDefinitionMap.getOrDefault(FieldDefinitionSectionName.Analysis_Fields.getName(), new ArrayList<>()).stream()
                        .collect(Collectors.toMap(FieldDefinition::getFieldName, field -> field));

        // case 1: WARNING if the auto-detected fieldType based on column data doesn’t match the User defined
        // fieldType of a Lattice Field(change field type of Country from text to number)
        FieldDefinition countryDefinition = customNameToCustomFieldDefinition.get("Country");
        Assert.assertNotNull(countryDefinition);
        // change field type of Country from text to number
        countryDefinition.setFieldType(UserDefinedType.NUMBER);
        ValidateFieldDefinitionsResponse validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "Default", "Text", "Contacts",
                fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Custom_Fields.getName(),
                "Country",
                FieldValidationMessage.MessageLevel.WARNING);

        // case 2: WARNING if time zone is ISO 8601 but column values are not or time zone is not ISO 8601 but column values are
        FieldDefinition createdDateDefinition =
                fieldNameToAnalysisFieldDefinition.get(InterfaceName.CreatedDate.name());
        Assert.assertNotNull(createdDateDefinition);
        createdDateDefinition.setTimeZone(TimeStampConvertUtils.SYSTEM_USER_TIME_ZONE);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions("Default", "Text", "Contacts",
                fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Analysis_Fields.getName(),
                InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.WARNING);

        // case 3: Compare Current Template Against Spec, Error for missing: remove created Date from Analysis Fields
        fieldNameToAnalysisFieldDefinition.remove(InterfaceName.CreatedDate.name());
        fieldDefinitionMap.put(FieldDefinitionSectionName.Analysis_Fields.getName(), new ArrayList<>(fieldNameToAnalysisFieldDefinition.values()));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions("Default", "Text", "Contacts",
                fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Analysis_Fields.getName(),
                InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.ERROR);


        // case 4: Compare Current Template Against Existing, Error for missing: remove "Country" from "Custom Fields"
        customNameToCustomFieldDefinition.remove("Country");
        fieldDefinitionMap.put(FieldDefinitionSectionName.Custom_Fields.getName(), new ArrayList<>(customNameToCustomFieldDefinition.values()));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions("Default", "Text", "Contacts",
                fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Custom_Fields.getName(),
                "Country",
                FieldValidationMessage.MessageLevel.ERROR);

        // case 5: If an additional field is found that is in the same section of the Existing Template, ERROR
        // in case 1 the country definition is in Custom Fields, add it to Analysis Fields section
        fieldNameToAnalysisFieldDefinition.put("Country", countryDefinition);
        fieldDefinitionMap.put(FieldDefinitionSectionName.Analysis_Fields.getName(), new ArrayList<>(fieldNameToAnalysisFieldDefinition.values()));
        System.out.println("test\n" + JsonUtils.pprint(validateRequest));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions("Default", "Text", "Contacts",
                fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Analysis_Fields.getName(),
                countryDefinition.getFieldName(),
                FieldValidationMessage.MessageLevel.ERROR);

        // case 6: If an additional field is found that is not in the Existing Template, ERROR
        String fakedFieldName = "fakedDefinition";
        FieldDefinition fakedDefinition = new FieldDefinition();
        fakedDefinition.setFieldName(fakedFieldName);
        fakedDefinition.setFieldType(UserDefinedType.TEXT);
        fieldNameToAnalysisFieldDefinition.put(fakedFieldName, fakedDefinition);
        fieldDefinitionMap.put(FieldDefinitionSectionName.Analysis_Fields.getName(), new ArrayList<>(fieldNameToAnalysisFieldDefinition.values()));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions("Default", "Text", "Contacts",
                fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Analysis_Fields.getName(),
                fakedFieldName,
                FieldValidationMessage.MessageLevel.ERROR);

    }


    private void setValidateRequestFromFetchResponse(FetchFieldDefinitionsResponse fetchResponse) {
        validateRequest.setCurrentFieldDefinitionsRecord(fetchResponse.getCurrentFieldDefinitionsRecord());
        validateRequest.setImportWorkflowSpec(fetchResponse.getImportWorkflowSpec());
        validateRequest.setAutodetectionResultsMap(fetchResponse.getAutodetectionResultsMap());
        validateRequest.setExistingFieldDefinitionsMap(fetchResponse.getExistingFieldDefinitionsMap());
        validateRequest.setOtherTemplateDataMap(fetchResponse.getOtherTemplateDataMap());
    }
}
