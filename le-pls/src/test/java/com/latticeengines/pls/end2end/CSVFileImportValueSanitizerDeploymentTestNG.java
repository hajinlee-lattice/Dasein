package com.latticeengines.pls.end2end;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

public class CSVFileImportValueSanitizerDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    protected static final String ACCOUNT_VALIDATOR_SOURCE_FILE = "Account_validator.csv";

    @BeforeClass(groups = "deployment.import.group2")
    public void setup() throws Exception {
        String featureFlag = LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG, flags);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        createDefaultImportSystem();
    }

    @Test(groups = "deployment.import.group2")
    public void testValueSanitizer() throws Exception {
        // import file
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_VALIDATOR_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_VALIDATOR_SOURCE_FILE));
        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("ID")) {
                fieldMapping.setIdType(FieldMapping.IdType.Account);
                fieldMapping.setSystemName(DEFAULT_SYSTEM);
                fieldMapping.setMapToLatticeId(true);
            }
            //remove id field.
            if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())) {
                fieldMapping.setMappedField(null);
            }
        }
        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                feedType);
        sourceFile = sourceFileService.findByName(sourceFile.getName());

        String dataFeedTaskId = cdlService.createS3Template(customerSpace, sourceFile.getName(),
                SOURCE, ENTITY_ACCOUNT, feedType, null, ENTITY_ACCOUNT + "Data");
        Assert.assertNotNull(sourceFile);
        Assert.assertNotNull(dataFeedTaskId);

        startCDLImport(sourceFile, ENTITY_ACCOUNT);

        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, dataFeedTaskId);
        Assert.assertNotNull(dataFeedTask);
        Assert.assertNotNull(dataFeedTask.getDataFeedTaskConfig());
        Assert.assertNotNull(dataFeedTask.getDataFeedTaskConfig().getSanitizers());

        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts",
                PathBuilder
                        .buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(mainTestTenant.getId()))
                        .toString(),
                SourceType.FILE.getName());
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, targetPath));
        String avroFileName = sourceFile.getName().substring(0,
                sourceFile.getName().lastIndexOf("."));
        List<String> avroFiles = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, targetPath, file ->
                !file.isDirectory() && file.getPath().toString().contains(avroFileName)
                        && file.getPath().getName().endsWith("avro"));
        Assert.assertEquals(avroFiles.size(), 1);

        List<GenericRecord> records = AvroUtils.getData(yarnConfiguration, new Path(avroFiles.get(0)));

        for (int i = 0; i < records.size(); i++) {
            if (i == 0) {
                Assert.assertEquals(records.get(i).get(InterfaceName.PhoneNumber.name()).toString(), "123-456-7890");
            } else {
                Assert.assertNull(records.get(i).get(InterfaceName.PhoneNumber.name()));
            }
        }
    }

}
