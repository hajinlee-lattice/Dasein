package com.latticeengines.pls.service.impl;

import java.util.HashMap;
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
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidation;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidationResult;
import com.latticeengines.domain.exposed.pls.frontend.ValidationCategory;
import com.latticeengines.pls.end2end.fileimport.CSVFileImportDeploymentTestNGBase;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.ModelingFileMetadataService;

public class ModelingFileMetadataServiceImplDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ModelingFileMetadataServiceImplDeploymentTestNG.class);

    @Inject
    private ModelingFileMetadataService modelingFileMetadataService;

    @Inject
    private CDLService cdlService;

    private static final String OTHER_SYSTEM = "Test_OtherSystem";


    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String flag = LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName();
        Map<String, Boolean> map = new HashMap<>();
        map.put(flag, Boolean.TRUE);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG, map);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        cdlService.createS3ImportSystem(mainTestTenant.getName(), DEFAULT_SYSTEM, S3ImportSystem.SystemType.Other,
                true);
    }

    @Test(groups = "deployment")
    public void verifyDateFormat() {
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_TRANSACTION), ENTITY_TRANSACTION, TRANSACTION_SOURCE_FILE_MISSING,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + TRANSACTION_SOURCE_FILE_MISSING));

        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_TRANSACTION);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), ENTITY_TRANSACTION, SOURCE, feedType);
        boolean dateExist = false;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
            if (fieldMapping.getUserField().equals("Date")) {
                dateExist = true;
                // 5/26/2016 12:00:00 AM
                Assert.assertEquals(fieldMapping.getDateFormatString(), "MM/DD/YYYY");
                Assert.assertEquals(fieldMapping.getTimeFormatString(), "00:00:00 12H");
                // set wrong date format and time zone
                fieldMapping.setDateFormatString("MM-DD-YYYY");
                fieldMapping.setTimezone(TimeStampConvertUtils.SYSTEM_USER_TIME_ZONE);
            }
        }
        Assert.assertTrue(dateExist);
        FieldValidationResult fieldValidationResult = modelingFileMetadataService
                .validateFieldMappings(sourceFile.getName(), fieldMappingDocument, ENTITY_TRANSACTION, SOURCE, feedType);
        List<FieldValidation> validations = fieldValidationResult.getFieldValidations();
        log.info("result is " + JsonUtils.pprint(validations));
        Assert.assertNotNull(validations);
        List<FieldValidation> errorValidations = validations.stream()
                .filter(validation -> FieldValidation.ValidationStatus.ERROR.equals(validation.getStatus()))
                .collect(Collectors.toList());
        Assert.assertNotNull(errorValidations);
        List<FieldValidation> warningValidations = validations.stream()
                .filter(validation -> FieldValidation.ValidationStatus.WARNING.equals(validation.getStatus()))
                .collect(Collectors.toList());
        Assert.assertNotNull(warningValidations);
        Assert.assertEquals(warningValidations.size(), 1);
        Map<ValidationCategory, List<FieldValidation>> groupedValidations =
                fieldValidationResult.getGroupedValidations();
        Assert.assertEquals(groupedValidations.get(ValidationCategory.DateFormat).size(), 1);
    }

    @Test(groups = "deployment")
    public void verifyStandardFields() {
        // create template
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));

        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        boolean idExist = false;
        boolean externalIdExist = false;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
            //  ID was mapped to mapped field CustomerAccountId, then CustomerAccountId was renamed to
            //  user_DefaultSystem_tldqc9rx_AccountId before saving in template
            if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())) {
                Assert.assertEquals(fieldMapping.getUserField(), "ID");
                fieldMapping.setIdType(FieldMapping.IdType.Account);
                fieldMapping.setMappedToLatticeSystem(false);
                idExist = true;
            }
            if ("CrmAccount_External_ID".equals(fieldMapping.getUserField())) {
                externalIdExist = true;
                Assert.assertNotNull(fieldMapping.getMappedField());
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.TEXT);
            }
        }

        Assert.assertTrue(idExist);
        Assert.assertTrue(externalIdExist);
        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                feedType);

        startCDLImport(sourceFile, ENTITY_ACCOUNT, DEFAULT_SYSTEM);

        // upload same file, then edit template
        sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));

        fieldMappingDocument = modelingFileMetadataService.getFieldMappingDocumentBestEffort(sourceFile.getName(),
                ENTITY_ACCOUNT, SOURCE, feedType);
        boolean longitudeExist = false;
        boolean latitudeExist = false;
        idExist = false;
        boolean websiteExist = false;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
            // unmap the standard field, this will trigger 2 warnings and one error
            if ("ID".equals(fieldMapping.getUserField())) {
                fieldMapping.setMappedField(null);
                idExist = true;
            }
            if (InterfaceName.Website.name().equals(fieldMapping.getMappedField())) {
                fieldMapping.setMappedField(null);
                websiteExist = true;
            }
            if ("Lattitude".equals(fieldMapping.getUserField())) {
                fieldMapping.setMappedField(InterfaceName.Longitude.name());
                longitudeExist = true;
            }
            if ("Longitude".equals(fieldMapping.getUserField())) {
                fieldMapping.setMappedField(InterfaceName.Latitude.name());
                latitudeExist = true;
            }
        }
        Assert.assertTrue(longitudeExist);
        Assert.assertTrue(latitudeExist);
        Assert.assertTrue(idExist);
        Assert.assertTrue(websiteExist);

        FieldValidationResult fieldValidationResult =
                modelingFileMetadataService.validateFieldMappings(sourceFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT,
                SOURCE, feedType);
        log.info(JsonUtils.pprint(fieldValidationResult));
        List<FieldValidation> validations = fieldValidationResult.getFieldValidations();
        List<FieldValidation> warningValidations = validations.stream()
                .filter(validation -> FieldValidation.ValidationStatus.WARNING.equals(validation.getStatus()))
                .collect(Collectors.toList());
        Assert.assertNotNull(warningValidations);
        Assert.assertEquals(warningValidations.size(), 6);

        Map<ValidationCategory, List<FieldValidation>> groupedValidations =
                fieldValidationResult.getGroupedValidations();
        Assert.assertEquals(groupedValidations.get(ValidationCategory.ColumnMapping).size(), 6);


        // verify error
        List<FieldValidation> errorValidations =
                validations.stream().filter(validation -> FieldValidation.ValidationStatus.ERROR.equals(validation.getStatus())).collect(Collectors.toList());
        Assert.assertNotNull(errorValidations);
        Assert.assertEquals(errorValidations.size(), 0);
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyStandardFields")
    public void testFieldMapping_WithOtherTemplate() {
        // create another system
        cdlService.createS3ImportSystem(mainTestTenant.getName(), OTHER_SYSTEM, S3ImportSystem.SystemType.Other,
                false);
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));

        String feedType = getFeedTypeByEntity(OTHER_SYSTEM, ENTITY_ACCOUNT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        boolean parentExternalIdExist = false;
        FieldMapping externalMapping = null;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
            // the type for CrmAccount_External_ID is TEXT in DEFAULT_SYSTEM, in this test,
            // try to set it to be NUMBER, this is the first error
            if ("CrmAccount_External_ID".equals(fieldMapping.getUserField())) {
                externalMapping = fieldMapping;
                Assert.assertNotNull(fieldMapping.getMappedField());
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.TEXT);
                fieldMapping.setFieldType(UserDefinedType.NUMBER);
            }
            // map user field to system DEFAULT_SYSTEM
            if ("Parent_External_ID".equals(fieldMapping.getUserField())) {
                parentExternalIdExist = true;
                fieldMapping.setSystemName(DEFAULT_SYSTEM);
                fieldMapping.setIdType(FieldMapping.IdType.Account);
            }
        }
        Assert.assertTrue(parentExternalIdExist);
        Assert.assertNotNull(externalMapping);

        FieldValidationResult fieldValidationResult =
                modelingFileMetadataService.validateFieldMappings(sourceFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT,
                        SOURCE, feedType);
        log.info("field result is: " + JsonUtils.pprint(fieldValidationResult));

        // one data type error which conflicts with the user field in default system,
        // the other one is set as NUMBER but appears to only have TEXT values
        Assert.assertEquals(fieldValidationResult.getGroupedValidations().get(ValidationCategory.DataType).size(), 2);

        // change back for create template
        externalMapping.setFieldType(UserDefinedType.TEXT);
        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT,
                SOURCE, feedType);
        startCDLImport(sourceFile, ENTITY_ACCOUNT, OTHER_SYSTEM);

        // edit the template
        sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));

        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        boolean accountForPlatform = false;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
            // map user field to DEFAULT_SYSTEM, this will cause  with setting when create template
            if ("S_Account_For_Platform_Test".equals(fieldMapping.getUserField())) {
                accountForPlatform = true;
                fieldMapping.setSystemName(DEFAULT_SYSTEM);
                fieldMapping.setIdType(FieldMapping.IdType.Account);
            }
        }
        Assert.assertTrue(accountForPlatform);
        fieldValidationResult = modelingFileMetadataService.validateFieldMappings(sourceFile.getName(),
                fieldMappingDocument, ENTITY_ACCOUNT, SOURCE, feedType);
        log.info("field result is: " + JsonUtils.pprint(fieldValidationResult));
        Map<ValidationCategory, List<FieldValidation>> groupedValidations =
                fieldValidationResult.getGroupedValidations();

        //1 error is Multiple user fields are mapped to standard field
        // 2. no field mapped to OTHER_SYSTEM, no unique id set
        Assert.assertEquals(groupedValidations.get(ValidationCategory.ColumnMapping).size(), 0);
    }
}
