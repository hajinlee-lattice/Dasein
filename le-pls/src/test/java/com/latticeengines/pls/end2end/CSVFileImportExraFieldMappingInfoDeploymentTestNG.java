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
import com.latticeengines.domain.exposed.pls.frontend.ExtraFieldMappingInfo;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

public class CSVFileImportExraFieldMappingInfoDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private static final String ACCOUNT_EXTRAINFO_FILE = "Account_ExtraInfo.csv";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testExtraFieldMappingInfo() {
        baseAccountFile = uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);

        Assert.assertNotNull(baseAccountFile);
        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT);
        String dfId = cdlService.createS3Template(customerSpace, baseAccountFile.getName(),
                SOURCE, ENTITY_ACCOUNT, feedType, null, ENTITY_ACCOUNT + "Data");

        SourceFile extraInfoSF = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_EXTRAINFO_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_EXTRAINFO_FILE));

        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(extraInfoSF.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        boolean columnExist = false;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            // html special character in user field is escaped
            if (fieldMapping.getUserField().equals("Test_HTML_&amp;")) {
                columnExist = true;
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.INTEGER);
            }
        }
        Assert.assertTrue(columnExist);
        ExtraFieldMappingInfo extraInfo = fieldMappingDocument.getExtraFieldMappingInfo();
        Assert.assertEquals(extraInfo.getMissedMappings().size(), 1);
        Assert.assertEquals(extraInfo.getNewMappings().size(), 2);
        Assert.assertTrue(extraInfo.getExistingMappings().size() > 1);

        modelingFileMetadataService.resolveMetadata(extraInfoSF.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                feedType);
        extraInfoSF = sourceFileService.findByName(extraInfoSF.getName());

        String dfIdExtra = cdlService.createS3Template(customerSpace, extraInfoSF.getName(),
                SOURCE, ENTITY_ACCOUNT, feedType, null, ENTITY_ACCOUNT + "Data");

        Assert.assertEquals(dfId, dfIdExtra);
    }
}
