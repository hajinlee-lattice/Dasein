package com.latticeengines.pls.end2end;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

public class CSVFileImportDateFormatDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {


    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testDateFormat() {
        baseAccountFile = uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
        String dateFormatString1 = "DD/MM/YYYY";
        String timezone1 = "UTC-5     America/New York, America/Lima";
        String dateFormatString2 = "MM.DD.YY";
        String timeFormatString2 = "00:00:00 24H";
        String timezone2 = "UTC+8     Asia/Shanghai, Australia/Perth";

        Assert.assertNotNull(baseAccountFile);

        String dfId = cdlService.createS3Template(customerSpace, baseAccountFile.getName(),
                SOURCE, ENTITY_ACCOUNT, ENTITY_ACCOUNT + FEED_TYPE_SUFFIX, null, ENTITY_ACCOUNT + "Data");

        SourceFile accountDateSF = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE_FROMATDATE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE_FROMATDATE));

        String feedType = ENTITY_ACCOUNT + FEED_TYPE_SUFFIX;
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(accountDateSF.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("TestDate1")) {
                fieldMapping.setFieldType(UserDefinedType.DATE);
                fieldMapping.setMappedToLatticeField(false);
                fieldMapping.setDateFormatString(dateFormatString1);
                fieldMapping.setTimezone(timezone1);
            } else if (fieldMapping.getUserField().equals("TestDate2")) {
                fieldMapping.setFieldType(UserDefinedType.DATE);
                fieldMapping.setMappedToLatticeField(false);
                fieldMapping.setDateFormatString(dateFormatString2);
                fieldMapping.setTimeFormatString(timeFormatString2);
                fieldMapping.setTimezone(timezone2);
            }
        }

        modelingFileMetadataService.resolveMetadata(accountDateSF.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                feedType);
        accountDateSF = sourceFileService.findByName(accountDateSF.getName());

        String dfIdExtra = cdlService.createS3Template(customerSpace, accountDateSF.getName(),
                SOURCE, ENTITY_ACCOUNT, ENTITY_ACCOUNT + FEED_TYPE_SUFFIX, null, ENTITY_ACCOUNT + "Data");

        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(accountDateSF.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("TestDate1")) {
                Assert.assertEquals(fieldMapping.getDateFormatString(), dateFormatString1);
                Assert.assertEquals(fieldMapping.getTimezone(), timezone1);
            } else if (fieldMapping.getUserField().equals("TestDate2")) {
                Assert.assertEquals(fieldMapping.getDateFormatString(), dateFormatString2);
                Assert.assertEquals(fieldMapping.getTimeFormatString(), timeFormatString2);
                Assert.assertEquals(fieldMapping.getTimezone(), timezone2);
            }
        }

        Assert.assertEquals(dfId, dfIdExtra);
    }
}
