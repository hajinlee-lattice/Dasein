package com.latticeengines.pls.service.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
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
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.util.ImportWorkflowUtilsTestNG;
import com.latticeengines.proxy.exposed.core.ImportWorkflowSpecProxy;

public class ModelingFileMetadataServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ModelingFileMetadataServiceImplDeploymentTestNG.class);

    @Inject
    private ModelingFileMetadataService modelingFileMetadataService;

    private String localPath = "com/latticeengines/pls/util/";
    private String csvFileName = "test-contact-import-file.csv";

    private BusinessEntity entity = BusinessEntity.Contact;
    private ValidateFieldDefinitionsRequest validateRequest = new ValidateFieldDefinitionsRequest();
    private String fileName;


    @Inject
    private ImportWorkflowSpecProxy importWorkflowSpecProxy;

    @Inject
    private FileUploadService fileUploadService;

    @Inject
    private CDLService cdlService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);

        //cdlService.createS3ImportSystem(mainTestTenant.getName(), "Default", S3ImportSystem.SystemType.Other, false);
        // Set up the import system used in this test.  This involves setting up the Account system ID as if the
        // Accounts template was already set up.  This is needed for Match to Accounts - ID to work in the Contacts
        // template set up which is tested here.
        S3ImportSystem importSystem = cdlService.getS3ImportSystem(mainTestTenant.getName(), "DefaultSystem");
        importSystem.setAccountSystemId(importSystem.generateAccountSystemId());
        cdlService.updateS3ImportSystem(mainTestTenant.getName(), importSystem);

        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(entity.name()), entity.name(), csvFileName,
                ClassLoader.getSystemResourceAsStream(localPath + csvFileName));

        fileName = sourceFile.getName();
        FetchFieldDefinitionsResponse  fetchResponse =  modelingFileMetadataService.fetchFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName);
        setValidateRequestFromFetchResponse(fetchResponse);
    }

    /**
     * existing check
    # ERROR if required field is missing from set of mapped Lattice Fields in the template (either in the original
     template or newly added during the import).
    # ERROR if multiple columnNames map to the same Spec (Lattice) Field.
    # ERROR if fieldType of Lattice Field in new template doesn’t match the Spec.
    # WARNING: If user does not map ColumnName that matches a Lattice Field to the corresponding Lattice Field.
    # WARNING if fieldType of Custom Field set by user doesn’t match the autodetected fieldType.
    # WARNING if the autodetected fieldType based on column data doesn’t match the User defined fieldType of a
     Lattice Field.
    Date Formats (For both Lattice and Custom Fields):
    # WARNING if data format selected by user can’t parse 10% or more of the date column rows and doesn’t match the
     autodetected date format.
    # WARNING if data format selected by user can’t parse 10% or more of the date columns’ rows even though it is the
     same as the autodetected date format
    Possible because autodetected format uses prior format in second round import if previous set.
    # WARNING if the data format selected by the user changed, even if it can parse more than 10% of the date columns’
     rows.
    # WARNING if time zone is ISO 8601 but column values are not or time zone is not ISO 8601 but column values are.
     new check
     1. Validation of the Presence of Fields
     A. Compare Current Template Against Spec:
     i. All Lattice Fields in Spec are in Current Template.
     a. may have inCurrentImport set to ‘false’.
     ii. Create an ERROR for missing fields.
     B. Compare Current Template Against Existing Template:
     i. All fields in the Existing Template are in the Current Template.
     a. may have inCurrentImport set to ‘false’.
     b. may have moved from Lattice Field Section to Custom Field Section.
     ii. Create an ERROR for missing fields.
     C. No additional fields are in Current Template Lattice Fields sections.
     i. If an additional field is found that is in the same section of the Existing Template, create an ERROR saying
     the field should be moved to the Custom Fields section.
     ii. If an additional field is found that is not in the Existing Template, create an ERROR saying a field not in
     the Spec is in a Lattice Field section.
     2. Validation of Field Types
     A.If no Existing Template and no existing Other Templates or Batch Store, allow fieldType to be set with no
     warning/error.
     B. If no Existing Template, but Other Template or Batch Store has field, fieldType must be set to match Other
     Template and/or Batch Store. Otherwise, create ERROR.
     C. If Existing Template and no existing Other Templates or Batch Store, allow fieldType to be changed but create
     WARNING.
     D. If Existing Template and Other Template or Batch Store has field, fieldType cannot be changed and must match
     Other Template and/or Batch Store. If not, issue ERROR.
     3. Validation of Date Formats
     A. Allow Current Template to change Date Format, Time Format, and Time Zone for Lattice and Custom Fields.
     i. Formats do not need to match Existing Template, Other Templates, or Batch Store.
     B. Check that at least Date Format is set for a DATE fieldType field. If not, create an ERROR.
     4. Make sure ID fields (Unique ID, Match IDs and Other IDs) are all TEXT type. If not, create an ERROR.
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
        Map<String, FieldDefinition> fieldNameToUniqueIDFieldDefinition =
                fieldDefinitionMap.getOrDefault(FieldDefinitionSectionName.Unique_ID.getName(),
                        new ArrayList<>()).stream().collect(Collectors.toMap(FieldDefinition::getFieldName,
                        field -> field));
        Map<String, FieldDefinition> fieldNameToContactFieldDefinition =
                fieldDefinitionMap.getOrDefault(FieldDefinitionSectionName.Contact_Fields.getName(),
                        new ArrayList<>()).stream().collect(
                                Collectors.toMap(FieldDefinition::getFieldName, field -> field));
        Map<String, FieldDefinition> fieldNameToAnalysisFieldDefinition =
                fieldDefinitionMap.getOrDefault(FieldDefinitionSectionName.Analysis_Fields.getName(),
                        new ArrayList<>()).stream().collect(
                                Collectors.toMap(FieldDefinition::getFieldName, field -> field));

        // case 1: required flag change, error
        FieldDefinition nameDefinition = fieldNameToContactFieldDefinition.get("FirstName");
        Assert.assertNotNull(nameDefinition);
        nameDefinition.setRequired(Boolean.FALSE);
        ValidateFieldDefinitionsResponse validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, validateRequest);

        Assert.assertNotNull(validateResponse);
        Assert.assertEquals(validateResponse.getValidationResult(),
                ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), InterfaceName.FirstName.name(),
                FieldValidationMessage.MessageLevel.ERROR, "Required flag is not the same for attribute First Name");

        // case 2: required field missing, error
        // Field name FirstName is required, needs set column name
        fieldNameToContactFieldDefinition.remove(InterfaceName.FirstName.name());
        fieldDefinitionMap.put(FieldDefinitionSectionName.Contact_Fields.getName(),
                new ArrayList<>(fieldNameToContactFieldDefinition.values()));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, validateRequest);
        Assert.assertEquals(validateResponse.getValidationResult(),
                ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), InterfaceName.FirstName.name(),
                FieldValidationMessage.MessageLevel.ERROR, "Field name FirstName is required, needs set column name");

        // case 3: multiple custom name map to same lattice fields
        // Last Name -> Last Name, LastName -> Last Name, in section "Contact Fields", custom filed "LastName" was
        // mapped lattice filed "Last Name", manually mapped "Last Name" to lattice field "Last Name"
        FieldDefinition lastNameDefinition = customNameToCustomFieldDefinition.get("Last Name");
        Assert.assertNotNull(lastNameDefinition);
        lastNameDefinition.setFieldName(InterfaceName.LastName.name());
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, validateRequest);
        Assert.assertEquals(validateResponse.getValidationResult(),
                ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), InterfaceName.LastName.name(),
                FieldValidationMessage.MessageLevel.ERROR, "Multiple custom fields are mapped to lattice field LastName");

        // case 4: unmap the column name(Last Name in "Contact Fields") that match lattice field
        FieldDefinition lastNameInContactField = fieldNameToContactFieldDefinition.get(InterfaceName.LastName.name());
        lastNameInContactField.setColumnName(null);
        lastNameInContactField.setInCurrentImport(false);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Contact_Fields.getName(),
                InterfaceName.LastName.name(), FieldValidationMessage.MessageLevel.WARNING, "Column name Last Name " +
                        "matched Lattice Field LastName, but they are not mapped to each other");

        // case 5: set fieldName LastName to empty
        lastNameInContactField.setFieldName(null);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), null,
                FieldValidationMessage.MessageLevel.ERROR, "FieldName shouldn't be empty in Contact Fields.");

        // case 6: change field type of Email from Text to Integer(current vs spec)
        FieldDefinition emailDefinition = fieldNameToContactFieldDefinition.get(InterfaceName.Email.name());
        Assert.assertNotNull(emailDefinition);
        Assert.assertEquals(emailDefinition.getFieldType(), UserDefinedType.TEXT);
        emailDefinition.setFieldType(UserDefinedType.INTEGER);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), InterfaceName.Email.name(),
                FieldValidationMessage.MessageLevel.ERROR, "the current template has fieldType INTEGER while the Spec" +
                        " has fieldType TEXT for field Email");

        // case 7: for the case above, the type for auto-detected should be Text, this case will issue warning
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), InterfaceName.Email.name(),
                FieldValidationMessage.MessageLevel.WARNING, "auto-detected fieldType TEXT based on " +
                        "column data EmailAddress doesn’t match the fieldType INTEGER of Email in current template " +
                        "in section Contact Fields.");

        // case 8: WARNING if fieldType of Custom Field set by user doesn’t match the auto-detected fieldType.(current
        // vs auto-detected in custom fields)
        FieldDefinition earningDefinition = customNameToCustomFieldDefinition.get("Earnings");
        Assert.assertNotNull(earningDefinition);
        // change field type from auto-detected number to text
        earningDefinition.setFieldType(UserDefinedType.TEXT);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Text", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Custom_Fields.getName(), "Earnings",
                FieldValidationMessage.MessageLevel.WARNING,
                "column Earnings is set as TEXT but appears to only have NUMBER values");

        // case 9: ID fields must have TEXT Field Type
        FieldDefinition idDefinition = fieldNameToUniqueIDFieldDefinition.get("CustomerContactId");
        Assert.assertNotNull(idDefinition);
        // change type for id to integer
        idDefinition.setFieldType(UserDefinedType.NUMBER);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Text", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Unique_ID.getName(), "CustomerContactId",
                FieldValidationMessage.MessageLevel.ERROR, "Field mapped to Contact Id in section Unique ID has type " +
                        "NUMBER but is required to have type Text.");

        // case 10: date format is not set when type is Date
        FieldDefinition createdDateDefinition =
                fieldNameToAnalysisFieldDefinition.get(InterfaceName.CreatedDate.name());
        Assert.assertNotNull(createdDateDefinition);
        createdDateDefinition.setDateFormat(null);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Text", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.ERROR,
                "Date Format shouldn't be empty for column CreatedDate with date type");

        // case 11: Date format selected by user can't parse > 10% of column data.
        createdDateDefinition.setDateFormat("MM-DD-YYYY");
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Text", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.WARNING,
                "CreatedDate is set to MM-DD-YYYY which can't parse the 01/01/2008 from uploaded file.");

        // case 12: Date format selected by user doesn't match autodetected date format.
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.WARNING, "CreatedDate is set to MM-DD-YYYY which is different from " +
        "auto-detected format MM/DD/YYYY.");

    }

    @Test(groups = "deployment", dependsOnMethods = "testFieldDefinitionValidate_noExistingTemplate")
    public void testFieldDefinitionValidate_withExistingTemplate() throws Exception {
        FieldDefinitionsRecord currentFieldDefinitionRecord = validateRequest.getCurrentFieldDefinitionsRecord();
        log.info("Committing fieldDefinitionsRecord:\n" + JsonUtils.pprint(currentFieldDefinitionRecord));
        FieldDefinitionsRecord commitRecord = modelingFileMetadataService.commitFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, false,
                currentFieldDefinitionRecord);
        FetchFieldDefinitionsResponse  fetchResponse =  modelingFileMetadataService.fetchFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName);
        setValidateRequestFromFetchResponse(fetchResponse);
        Assert.assertNotNull(commitRecord);


        //the second round test after the fetch api
        currentFieldDefinitionRecord = validateRequest.getCurrentFieldDefinitionsRecord();
        Map<String, List<FieldDefinition>> fieldDefinitionMap =
                currentFieldDefinitionRecord.getFieldDefinitionsRecordsMap();
        Map<String, FieldDefinition> customNameToCustomFieldDefinition =
                fieldDefinitionMap.getOrDefault(FieldDefinitionSectionName.Custom_Fields.getName(),
                        new ArrayList<>()).stream().collect(
                                Collectors.toMap(FieldDefinition::getColumnName, field -> field));
        Map<String, FieldDefinition> fieldNameToAnalysisFieldDefinition =
                fieldDefinitionMap.getOrDefault(FieldDefinitionSectionName.Analysis_Fields.getName(),
                        new ArrayList<>()).stream().collect(
                                Collectors.toMap(FieldDefinition::getFieldName, field -> field));

        // case 1: WARNING if the auto-detected fieldType based on column data doesn’t match the User defined in
        // custom fields
        // fieldType of a Lattice Field(change field type of Country from text to number)
        FieldDefinition countryDefinition = customNameToCustomFieldDefinition.get("Country");
        Assert.assertNotNull(countryDefinition);
        // change field type of Country from text to number
        countryDefinition.setFieldType(UserDefinedType.NUMBER);
        ValidateFieldDefinitionsResponse validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts",
                fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Custom_Fields.getName(), "Country", FieldValidationMessage.MessageLevel.WARNING,
                "column Country is set as NUMBER but appears to only have TEXT values");

        // case 2: Current Field Type doesn’t match Existing Template (no Batch Store or Other Template exists).
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Custom_Fields.getName(), "Country", FieldValidationMessage.MessageLevel.WARNING,
                "the field type for existing field mapping custom name user_Country -> field name Country " +
                        "will be changed to NUMBER from TEXT");

        // case 3: WARNING if time zone is ISO 8601 but column values are not or time zone is not ISO 8601 but column
        // values are
        FieldDefinition createdDateDefinition =
                fieldNameToAnalysisFieldDefinition.get(InterfaceName.CreatedDate.name());
        Assert.assertNotNull(createdDateDefinition);
        createdDateDefinition.setTimeZone(TimeStampConvertUtils.SYSTEM_USER_TIME_ZONE);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.WARNING,
                "Time zone should be part of value but is not for column CreatedDate.");

        // case 4: Date format selected by user doesn't match existing data format.
        // date format for existing template is MM-DD-YYYY set in above method
        createdDateDefinition.setDateFormat("MM.DD.YYYY");
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.WARNING, "CreatedDate is set to MM.DD.YYYY which is not " +
                        "consistent with existing template format MM-DD-YYYY.");

        // case 5: Compare Current Template Against Spec, Error for missing: remove created Date from Analysis Fields
        fieldNameToAnalysisFieldDefinition.remove(InterfaceName.CreatedDate.name());
        fieldDefinitionMap.put(FieldDefinitionSectionName.Analysis_Fields.getName(),
                new ArrayList<>(fieldNameToAnalysisFieldDefinition.values()));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.ERROR, "Field name CreatedDate in spec not in current template.");


        // case 6: Compare Current Template Against Existing, Error for missing: remove "Country" from "Custom Fields"
        customNameToCustomFieldDefinition.remove("Country");
        fieldDefinitionMap.put(FieldDefinitionSectionName.Custom_Fields.getName(),
                new ArrayList<>(customNameToCustomFieldDefinition.values()));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Custom_Fields.getName(),
                "Country", FieldValidationMessage.MessageLevel.ERROR,
                "Existing field user_Country mapped to column Country cannot be removed.");

        // case 7: If an additional field is found that is in the same section of the Existing Template, ERROR
        // in case 1 the country definition is in Custom Fields, add it to Analysis Fields section
        fieldNameToAnalysisFieldDefinition.put("Country", countryDefinition);
        fieldDefinitionMap.put(FieldDefinitionSectionName.Analysis_Fields.getName(),
                new ArrayList<>(fieldNameToAnalysisFieldDefinition.values()));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), countryDefinition.getFieldName(),
                FieldValidationMessage.MessageLevel.ERROR, "field name user_Country of Analysis Fields in template " +
                        "not in spec should be moved to Custom Fields section.");

        // case 8: If an additional field is found that is not in the Existing Template, ERROR
        String fakedFieldName = "fakedDefinition";
        FieldDefinition fakedDefinition = new FieldDefinition();
        fakedDefinition.setFieldName(fakedFieldName);
        fakedDefinition.setFieldType(UserDefinedType.TEXT);
        fieldNameToAnalysisFieldDefinition.put(fakedFieldName, fakedDefinition);
        fieldDefinitionMap.put(FieldDefinitionSectionName.Analysis_Fields.getName(),
                new ArrayList<>(fieldNameToAnalysisFieldDefinition.values()));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                "DefaultSystem", "Test", "Contacts", fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), fakedFieldName,
                FieldValidationMessage.MessageLevel.ERROR,
                "field name fakedDefinition not in spec is in Analysis Fields section.");

    }



    @Test(groups = "deployment")
    public void testValidateIndividualSpec() throws Exception {

        String tenantId = MultiTenantContext.getShortTenantId();
        ImportWorkflowSpec testSpec = importWorkflowSpecProxy.getImportWorkflowSpec(tenantId, "other", "contacts");

        log.error("Expected import workflow spec is:\n" + JsonUtils.pprint(testSpec));
        InputStream specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());

        // case 1: input the same spec in S3
        List<String> errors  = modelingFileMetadataService.validateIndividualSpec("other", "contacts",
                specInputStream);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("input spec matches the existing spec with system type other and " +
                "system object contacts"));

        Map<String, List<FieldDefinition>> recordsMap = testSpec.getFieldDefinitionsRecordsMap();
        Assert.assertNotNull(recordsMap);
        Map<String, FieldDefinition> fieldNameToDefinition =
                recordsMap.values().stream().flatMap(List::stream).collect(Collectors.toMap(FieldDefinition::getFieldName,
                        e -> e));

        // case 2: field definition has same matching column name
        FieldDefinition firstNameDefinition = fieldNameToDefinition.get("FirstName");
        firstNameDefinition.setMatchingColumnNames(Arrays.asList("First Name", "First Name"));
        specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        errors  = modelingFileMetadataService.validateIndividualSpec("other", "contacts",
                specInputStream);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("duplicates found in matching column for field name FirstName"));

        // case 2.b duplicate column name across the field definition
        FieldDefinition lastNameDefinition = fieldNameToDefinition.get("LastName");
        lastNameDefinition.setMatchingColumnNames(Arrays.asList("First Name", "LAST NAME"));
        errors  = modelingFileMetadataService.validateIndividualSpec("other", "contacts",
                specInputStream);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("duplicates found in matching column for field name LastName"));

        // case 3: required flag
        firstNameDefinition.setRequired(null);
        specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        errors  = modelingFileMetadataService.validateIndividualSpec("other", "contacts",
                specInputStream);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("required flag should be set for FirstName"));

        // case 4: field type change
        firstNameDefinition.setFieldType(UserDefinedType.NUMBER);
        specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        errors  = modelingFileMetadataService.validateIndividualSpec("other", "contacts",
                specInputStream);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("Physical type TEXT of the FieldDefinition with " +
                "same field name FirstName cannot be changed to NUMBER for system type other and system object " +
                "contacts"));
        Assert.assertTrue(errors.contains("Physical type NUMBER of the field name FirstName in the current " +
                "systemType other and systemObject contacts cannot be different than the Physical " +
                        "type TEXT in other template with system type test and system object contacts"));

        // case 5: two field definition has same field name
        System.out.println(JsonUtils.pprint(recordsMap));
        List<FieldDefinition> contactFieldDefinitions = recordsMap.get(FieldDefinitionSectionName.Contact_Fields.getName());
        Assert.assertNotNull(contactFieldDefinitions);
        FieldDefinition firstNameDefinition2 = generateFieldDefinition("FirstName", UserDefinedType.TEXT, Arrays.asList(
                "First Name"));
        contactFieldDefinitions.add(firstNameDefinition2);
        specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        errors  = modelingFileMetadataService.validateIndividualSpec("other", "contacts",
                specInputStream);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("field definitions have same field name FirstName"));

    }

    private FieldDefinition generateFieldDefinition(String fieldName, UserDefinedType type, List<String> matchingColumns) {
        FieldDefinition definition = new FieldDefinition();
        definition.setFieldType(type);
        definition.setMatchingColumnNames(matchingColumns);
        definition.setFieldName(fieldName);
        definition.setRequired(true);
        return definition;
    }

    private void setValidateRequestFromFetchResponse(FetchFieldDefinitionsResponse fetchResponse) {
        validateRequest.setCurrentFieldDefinitionsRecord(fetchResponse.getCurrentFieldDefinitionsRecord());
        validateRequest.setImportWorkflowSpec(fetchResponse.getImportWorkflowSpec());
        validateRequest.setAutodetectionResultsMap(fetchResponse.getAutodetectionResultsMap());
        validateRequest.setExistingFieldDefinitionsMap(fetchResponse.getExistingFieldDefinitionsMap());
        validateRequest.setOtherTemplateDataMap(fetchResponse.getOtherTemplateDataMap());
    }




}
