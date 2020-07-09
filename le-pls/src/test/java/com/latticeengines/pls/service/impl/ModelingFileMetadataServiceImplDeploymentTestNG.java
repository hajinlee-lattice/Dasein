package com.latticeengines.pls.service.impl;

import java.util.List;
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
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidation;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidationResult;
import com.latticeengines.pls.end2end.CSVFileImportDeploymentTestNGBase;
import com.latticeengines.pls.service.ModelingFileMetadataService;

public class ModelingFileMetadataServiceImplDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ModelingFileMetadataServiceImplDeploymentTestNG.class);
    @Inject
    private ModelingFileMetadataService modelingFileMetadataService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment", enabled = false)
    public void verifyFieldMappingValidations() {
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
    }

    @Test(groups = "deployment")
    public void verifyStandardFields() {
        // create template
        SourceFile iniSourceFile = uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
        startCDLImport(iniSourceFile, ENTITY_ACCOUNT);

        // upload same file, then modify mapping
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));

        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT);
        FieldMappingDocument fieldMappingDocument =
                modelingFileMetadataService.getFieldMappingDocumentBestEffort(sourceFile.getName(), ENTITY_ACCOUNT,
                        SOURCE, feedType);
        boolean longitudeExist = false;
        boolean latitudeExist = false;
        boolean accountIdExist = false;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
            // unmap the standard field
            if (InterfaceName.AccountId.name().equals(fieldMapping.getMappedField())) {
                fieldMapping.setUserField(null);
                accountIdExist = true;
            }
            if (fieldMapping.getUserField().equals("Lattitude")) {
                fieldMapping.setMappedField(InterfaceName.Longitude.name());
                longitudeExist = true;
            }
            if (fieldMapping.getUserField().equals("Longitude")) {
                fieldMapping.setMappedField(InterfaceName.Latitude.name());
                latitudeExist = true;
            }
        }
        Assert.assertTrue(longitudeExist);
        Assert.assertTrue(latitudeExist);
        Assert.assertTrue(accountIdExist);

        FieldValidationResult fieldValidationResult =
                modelingFileMetadataService.validateFieldMappings(sourceFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT,
                SOURCE, feedType);
        log.info(JsonUtils.pprint(fieldValidationResult));
        List<FieldValidation> validations = fieldValidationResult.getFieldValidations();
        List<FieldValidation> warningValidations = validations.stream()
                .filter(validation -> FieldValidation.ValidationStatus.WARNING.equals(validation.getStatus()))
                .collect(Collectors.toList());
        Assert.assertNotNull(warningValidations);
        Assert.assertEquals(warningValidations.size(), 2);

        // verify error
        List<FieldValidation> errorValidations =
                validations.stream().filter(validation -> FieldValidation.ValidationStatus.ERROR.equals(validation.getStatus())).collect(Collectors.toList());
        Assert.assertNotNull(errorValidations);
        Assert.assertEquals(errorValidations.size(), 1);

        try {
            modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT,
                    SOURCE,
                    feedType);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof LedpException);
            LedpException ledp = (LedpException) e;
            Assert.assertEquals(ledp.getCode(), LedpCode.LEDP_18248);
        }
    }
}
