package com.latticeengines.pls.end2end;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

public class CSVFileImportTemplateResetDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private String contactDFId;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testResetTemplate() {
        setupTemplateAndData();
        DataFeedTask contactDFT = dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), contactDFId);
        Assert.assertNotNull(contactDFT);
        cdlService.resetTemplate(mainTestTenant.getId(), contactDFT.getFeedType(), false);
        contactDFT = dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), contactDFId);
        Assert.assertNull(contactDFT);

        Assert.assertThrows(LedpException.class, () -> cdlService.resetTemplate(mainTestTenant.getId(), accountDataFeedTask.getFeedType(), false));

        accountDataFeedTask = dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), accountDataFeedTask.getUniqueId());
        Assert.assertNotNull(accountDataFeedTask);
        cdlService.resetTemplate(mainTestTenant.getId(), accountDataFeedTask.getFeedType(), true);
        accountDataFeedTask = dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), accountDataFeedTask.getUniqueId());
        Assert.assertNull(accountDataFeedTask);
    }

    private void setupTemplateAndData() {
        prepareBaseData(ENTITY_ACCOUNT);
        getDataFeedTask(ENTITY_ACCOUNT);
        contactDFId = createDefaultContactTemplate();
    }

    private String createDefaultContactTemplate() {
        SourceFile defaultContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        String defaultFeedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_CONTACT);

        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(defaultContactFile.getName(), ENTITY_CONTACT, SOURCE, defaultFeedType);

        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("CrmAccount_External_ID")) {
                fieldMapping.setIdType(FieldMapping.IdType.Account);
            }
        }

        modelingFileMetadataService.resolveMetadata(defaultContactFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                defaultFeedType);
        defaultContactFile = sourceFileService.findByName(defaultContactFile.getName());

        String defaultDFId = cdlService.createS3Template(customerSpace, defaultContactFile.getName(),
                SOURCE, ENTITY_ACCOUNT, defaultFeedType, null, ENTITY_CONTACT + "Data");
        Assert.assertNotNull(defaultContactFile);
        Assert.assertNotNull(defaultDFId);
        return defaultDFId;
    }
}
