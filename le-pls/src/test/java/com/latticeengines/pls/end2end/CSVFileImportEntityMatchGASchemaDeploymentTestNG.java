package com.latticeengines.pls.end2end;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

public class CSVFileImportEntityMatchGASchemaDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private static final String ACCOUNT_ENTITY_MATCH_FILE = "Account_base.csv";

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String featureFlag = LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG, flags);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testFieldMappingAndTemplate() {
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_ENTITY_MATCH_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_ENTITY_MATCH_FILE));
        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        boolean containsAccountId = false;
        boolean containsCustomerAccountId = false;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (InterfaceName.AccountId.name().equals(fieldMapping.getMappedField())) {
                containsAccountId = true;
            }
            if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())) {
                containsCustomerAccountId = true;
            }
        }
        Assert.assertTrue(containsAccountId);
        Assert.assertFalse(containsCustomerAccountId);

        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                feedType);
        sourceFile = sourceFileService.findByName(sourceFile.getName());

        String dfIdExtra = cdlService.createS3Template(customerSpace, sourceFile.getName(),
                SOURCE, ENTITY_ACCOUNT, feedType, null, ENTITY_ACCOUNT + "Data");

        Assert.assertNotNull(dfIdExtra);

        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, dfIdExtra);
        Assert.assertNotNull(dataFeedTask);
        Attribute accountId = dataFeedTask.getImportTemplate().getAttribute(InterfaceName.CustomerAccountId);
        Assert.assertNotNull(accountId);
        Assert.assertNull(dataFeedTask.getImportTemplate().getAttribute(InterfaceName.AccountId));
        Assert.assertTrue(accountId.getRequired());
        Assert.assertFalse(accountId.getNullable());
    }
}
