package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
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
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
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
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.util.ImportWorkflowUtilsTestNG;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
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
    private static String otherSystemName = "Test_OtherSystem";
    private static String defaultSystemName = "DefaultSystem";
    private static String systemType = "Test";
    private static String systemObject = "Contacts";


    @Inject
    private ImportWorkflowSpecProxy importWorkflowSpecProxy;

    @Inject
    private FileUploadService fileUploadService;

    @Inject
    private CDLService cdlService;

    @Inject
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        //cdlService.createS3ImportSystem(mainTestTenant.getName(), "Default", S3ImportSystem.SystemType.Other, false);
        // Set up the import system used in this test.  This involves setting up the Account system ID as if the
        // Accounts template was already set up.  This is needed for Match to Accounts - ID to work in the Contacts
        // template set up which is tested here.
        S3ImportSystem importSystem = cdlService.getS3ImportSystem(mainTestTenant.getName(), defaultSystemName);
        importSystem.setAccountSystemId(importSystem.generateAccountSystemId());
        cdlService.updateS3ImportSystem(mainTestTenant.getName(), importSystem);

        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(entity.name()), entity.name(), csvFileName,
                ClassLoader.getSystemResourceAsStream(localPath + csvFileName));
        fileName = sourceFile.getName();
        FetchFieldDefinitionsResponse  fetchResponse =  modelingFileMetadataService.fetchFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName);
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
    # Error if the autodetected fieldType based on column data doesn’t match the User defined fieldType of a
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
                defaultSystemName, systemType, systemObject, fileName, validateRequest);

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
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
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
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
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
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Contact_Fields.getName(),
                InterfaceName.LastName.name(), FieldValidationMessage.MessageLevel.WARNING, "Column name Last Name " +
                        "matched Lattice Field LastName, but they are not mapped to each other");

        // case 5: set fieldName LastName to empty
        lastNameInContactField.setFieldName(null);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), null,
                FieldValidationMessage.MessageLevel.ERROR, "FieldName shouldn't be empty in Contact Fields.");

        // case 6: change field type of Email from Text to Integer(current vs spec)
        FieldDefinition emailDefinition = fieldNameToContactFieldDefinition.get(InterfaceName.Email.name());
        Assert.assertNotNull(emailDefinition);
        Assert.assertEquals(emailDefinition.getFieldType(), UserDefinedType.TEXT);
        emailDefinition.setFieldType(UserDefinedType.INTEGER);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), InterfaceName.Email.name(),
                FieldValidationMessage.MessageLevel.ERROR, "the current template has fieldType INTEGER while the Spec" +
                        " has fieldType TEXT for field Email");

        // case 7: for the case above, the type for auto-detected should be Text, this case will issue Error
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), InterfaceName.Email.name(),
                FieldValidationMessage.MessageLevel.ERROR, "auto-detected fieldType TEXT based on " +
                        "column data EmailAddress doesn’t match the fieldType INTEGER of Email in current template " +
                        "in section Contact Fields.");
        // change FieldType for emailDefinition back to TEXT for case 8
        emailDefinition.setFieldType(UserDefinedType.TEXT);

        // case 8: change field type of DoNotCall from Boolean to Text in AutoDetection result
        Map<String, FieldDefinition> autoDetectionResult = validateRequest.getAutodetectionResultsMap();
        FieldDefinition emailInAutoDetection = autoDetectionResult.get("EmailAddress");
        Assert.assertNotNull(emailInAutoDetection);
        emailInAutoDetection.setFieldType(UserDefinedType.INTEGER);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Contact_Fields.getName(), InterfaceName.Email.name(),
                FieldValidationMessage.MessageLevel.WARNING, "auto-detected fieldType INTEGER based on " +
                        "column data EmailAddress doesn’t match the fieldType TEXT of Email in current template " +
                        "in section Contact Fields.");

        // case 9: WARNING if fieldType of Custom Field set by user doesn’t match the auto-detected fieldType.(current
        // vs auto-detected in custom fields)
        FieldDefinition earningDefinition = customNameToCustomFieldDefinition.get("Earnings");
        Assert.assertNotNull(earningDefinition);
        // change field type from auto-detected number to text
        earningDefinition.setFieldType(UserDefinedType.TEXT);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Custom_Fields.getName(), "Earnings",
                FieldValidationMessage.MessageLevel.WARNING,
                "column Earnings is set as TEXT but appears to only have NUMBER values");

        // case 10: ID fields must have TEXT Field Type
        FieldDefinition idDefinition = fieldNameToUniqueIDFieldDefinition.get("CustomerContactId");
        Assert.assertNotNull(idDefinition);
        // change type for id to integer
        idDefinition.setFieldType(UserDefinedType.NUMBER);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Unique_ID.getName(), "CustomerContactId",
                FieldValidationMessage.MessageLevel.ERROR, "Field mapped to Contact Id in section Unique ID has type " +
                        "NUMBER but is required to have type Text.");

        // case 11: date format is not set when type is Date
        FieldDefinition createdDateDefinition =
                fieldNameToAnalysisFieldDefinition.get(InterfaceName.CreatedDate.name());
        Assert.assertNotNull(createdDateDefinition);
        createdDateDefinition.setDateFormat(null);
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.ERROR,
                "Date Format shouldn't be empty for column CreatedDate with date type");

        // case 12: Date format selected by user can't parse > 10% of column data.
        createdDateDefinition.setDateFormat("MM-DD-YYYY");
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.WARNING,
                "CreatedDate is set to MM-DD-YYYY which can't parse the 01/01/2008 from uploaded file.");

        // case 13: Date format selected by user doesn't match autodetected date format.
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.WARNING, "CreatedDate is set to MM-DD-YYYY which is different from " +
        "auto-detected format MM/DD/YYYY.");

    }

    @Test(groups = "deployment", dependsOnMethods = "testFieldDefinitionValidate_noExistingTemplate")
    public void testCDLExternalSystem() {
        FieldDefinitionsRecord currentFieldDefinitionRecord = validateRequest.getCurrentFieldDefinitionsRecord();
        // test for cdl external system
        List<FieldDefinition> otherIdsDefinitions =
                currentFieldDefinitionRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Other_IDs.getName());
        // add some other ID definition
        FieldDefinition otherID1 = new FieldDefinition();
        otherID1.setExternalSystemType(CDLExternalSystemType.CRM);
        otherID1.setFieldName("salesforce1");
        otherID1.setColumnName("column1");
        otherID1.setFieldType(UserDefinedType.TEXT);
        otherIdsDefinitions.add(otherID1);
        FieldDefinition otherID2 = new FieldDefinition();
        otherID2.setFieldName("facebook");
        otherID2.setColumnName("column2");
        otherID2.setFieldType(UserDefinedType.TEXT);
        otherID2.setExternalSystemType(CDLExternalSystemType.ERP);
        otherIdsDefinitions.add(otherID2);
        FieldDefinition otherID3 = new FieldDefinition();
        otherID3.setColumnName("id3");
        otherIdsDefinitions.add(otherID3);
        otherID3.setFieldType(UserDefinedType.TEXT);
        log.info("Committing fieldDefinitionsRecord:\n" + JsonUtils.pprint(currentFieldDefinitionRecord));

        FieldDefinitionsRecord commitRecord = modelingFileMetadataService.commitFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName, false,
                currentFieldDefinitionRecord);
        Assert.assertNotNull(commitRecord);
        // verify CDL external system
        CDLExternalSystem cdlExternalSystem =
                cdlExternalSystemProxy.getCDLExternalSystem(MultiTenantContext.getShortTenantId(),
                        BusinessEntity.Contact.toString());
        Assert.assertNotNull(cdlExternalSystem.getCRMIdList());
        Assert.assertEquals(cdlExternalSystem.getCRMIdList().size(), 1);
        Assert.assertNotNull(cdlExternalSystem.getERPIdList());
        Assert.assertEquals(cdlExternalSystem.getERPIdList().size(), 1);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCDLExternalSystem")
    public void testFieldDefinitionValidate_withExistingTemplate() throws Exception {
        FetchFieldDefinitionsResponse  fetchResponse =  modelingFileMetadataService.fetchFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName);
        setValidateRequestFromFetchResponse(fetchResponse);

        log.info("Committing validate request:\n" + JsonUtils.pprint(validateRequest));

        //the second round test after the fetch api
        FieldDefinitionsRecord currentFieldDefinitionRecord =
                validateRequest.getCurrentFieldDefinitionsRecord();
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
                defaultSystemName, systemType, systemObject,
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
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.WARNING,
                "Time zone should be part of value but is not for column CreatedDate.");

        // case 4: Date format selected by user doesn't match existing data format.
        // date format for existing template is MM-DD-YYYY set in above method
        createdDateDefinition.setDateFormat("MM.DD.YYYY");
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.WARNING, "CreatedDate is set to MM.DD.YYYY which is not " +
                        "consistent with existing template format MM-DD-YYYY.");

        // case 5: Compare Current Template Against Spec, Error for missing: remove created Date from Analysis Fields
        fieldNameToAnalysisFieldDefinition.remove(InterfaceName.CreatedDate.name());
        fieldDefinitionMap.put(FieldDefinitionSectionName.Analysis_Fields.getName(),
                new ArrayList<>(fieldNameToAnalysisFieldDefinition.values()));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), InterfaceName.CreatedDate.name(),
                FieldValidationMessage.MessageLevel.ERROR, "Field name CreatedDate in spec not in current template.");


        // case 6: Compare Current Template Against Existing, Error for missing: remove "Country" from "Custom Fields"
        customNameToCustomFieldDefinition.remove("Country");
        fieldDefinitionMap.put(FieldDefinitionSectionName.Custom_Fields.getName(),
                new ArrayList<>(customNameToCustomFieldDefinition.values()));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse, FieldDefinitionSectionName.Custom_Fields.getName(),
                "Country", FieldValidationMessage.MessageLevel.ERROR,
                "Existing field user_Country mapped to column Country cannot be removed.");

        // case 7: If an additional field is found that is in the same section of the Existing Template, ERROR
        // in case 1 the country definition is in Custom Fields, add it to Analysis Fields section
        fieldNameToAnalysisFieldDefinition.put("Country", countryDefinition);
        fieldDefinitionMap.put(FieldDefinitionSectionName.Analysis_Fields.getName(),
                new ArrayList<>(fieldNameToAnalysisFieldDefinition.values()));
        validateResponse = modelingFileMetadataService.validateFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
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
                defaultSystemName, systemType, systemObject, fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Analysis_Fields.getName(), fakedFieldName,
                FieldValidationMessage.MessageLevel.ERROR,
                "field name fakedDefinition not in spec is in Analysis Fields section.");

    }
    
    @Test(groups = "deployment", dependsOnMethods = "testFieldDefinitionValidate_withExistingTemplate")
    public void testFieldDefinitionValidate__WithOtherTemplate() throws Exception {
        // create another system and corresponding data feed task with same entity
        cdlService.createS3ImportSystem(mainTestTenant.getName(), otherSystemName, S3ImportSystem.SystemType.Other,
                false);
        S3ImportSystem otherSystem = cdlService.getS3ImportSystem(mainTestTenant.getName(), otherSystemName);
        otherSystem.setAccountSystemId(otherSystem.generateAccountSystemId());
        cdlService.updateS3ImportSystem(mainTestTenant.getName(), otherSystem);
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(entity.name()), entity.name(), csvFileName,
                ClassLoader.getSystemResourceAsStream(localPath + csvFileName));
        FetchFieldDefinitionsResponse fetchResponse = modelingFileMetadataService.fetchFieldDefinitions(
                otherSystemName, systemType, systemObject, sourceFile.getName());

        // verify the other template was not set up, field name was empty for user fields, but can predict the field
        // name user_X will have conflict with structure OtherTemplateDataMap
        // change field type of Country from text to number
        setValidateRequestFromFetchResponse(fetchResponse);
        FieldDefinitionsRecord currentFieldDefinitionRecord = validateRequest.getCurrentFieldDefinitionsRecord();
        Map<String, List<FieldDefinition>> fieldDefinitionMap =
                currentFieldDefinitionRecord.getFieldDefinitionsRecordsMap();
        Map<String, FieldDefinition> customNameToCustomFieldDefinition =
                fieldDefinitionMap.getOrDefault(FieldDefinitionSectionName.Custom_Fields.getName(),
                        new ArrayList<>()).stream().collect(
                        Collectors.toMap(FieldDefinition::getColumnName, field -> field));
        FieldDefinition countryDefinition = customNameToCustomFieldDefinition.get("Country");
        Assert.assertNotNull(countryDefinition);
        countryDefinition.setFieldType(UserDefinedType.NUMBER);
        ValidateFieldDefinitionsResponse validateResponse =
                modelingFileMetadataService.validateFieldDefinitions(defaultSystemName, systemType, systemObject,
                        fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Custom_Fields.getName(), "Country", FieldValidationMessage.MessageLevel.ERROR,
                "Field Type NUMBER is not consistent with field type TEXT in batch store or other template for user_Country.");
        countryDefinition.setFieldType(UserDefinedType.TEXT);

        modelingFileMetadataService.commitFieldDefinitions(otherSystemName, systemType, systemObject,
                sourceFile.getName(), false, fetchResponse.getCurrentFieldDefinitionsRecord());

        // validate type after having other template data in request, then field name has been user_X
        fetchResponse =  modelingFileMetadataService.fetchFieldDefinitions(
                defaultSystemName, systemType, systemObject, fileName);
        setValidateRequestFromFetchResponse(fetchResponse);

        currentFieldDefinitionRecord = validateRequest.getCurrentFieldDefinitionsRecord();
        fieldDefinitionMap =
                currentFieldDefinitionRecord.getFieldDefinitionsRecordsMap();
        customNameToCustomFieldDefinition =
                fieldDefinitionMap.getOrDefault(FieldDefinitionSectionName.Custom_Fields.getName(),
                        new ArrayList<>()).stream().collect(
                        Collectors.toMap(FieldDefinition::getColumnName, field -> field));

        countryDefinition = customNameToCustomFieldDefinition.get("Country");
        Assert.assertNotNull(countryDefinition);
        // change field type of Country from text to number
        countryDefinition.setFieldType(UserDefinedType.NUMBER);
        validateResponse =
                modelingFileMetadataService.validateFieldDefinitions(defaultSystemName, systemType, systemObject,
                fileName, validateRequest);
        ImportWorkflowUtilsTestNG.checkGeneratedResult(validateResponse,
                FieldDefinitionSectionName.Custom_Fields.getName(), "Country", FieldValidationMessage.MessageLevel.ERROR,
                "Field Type NUMBER is not consistent with field type TEXT in batch store or other template for user_Country.");

    }


    private void setValidateRequestFromFetchResponse(FetchFieldDefinitionsResponse fetchResponse) {
        validateRequest.setCurrentFieldDefinitionsRecord(fetchResponse.getCurrentFieldDefinitionsRecord());
        validateRequest.setImportWorkflowSpec(fetchResponse.getImportWorkflowSpec());
        validateRequest.setAutodetectionResultsMap(fetchResponse.getAutodetectionResultsMap());
        validateRequest.setExistingFieldDefinitionsMap(fetchResponse.getExistingFieldDefinitionsMap());
        validateRequest.setOtherTemplateDataMap(fetchResponse.getOtherTemplateDataMap());
    }




}
