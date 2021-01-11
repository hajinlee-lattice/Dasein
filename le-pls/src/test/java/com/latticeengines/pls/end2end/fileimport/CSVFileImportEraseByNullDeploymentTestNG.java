package com.latticeengines.pls.end2end.fileimport;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class CSVFileImportEraseByNullDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CSVFileImportEraseByNullDeploymentTestNG.class);

    @Inject
    private MetadataProxy metadataProxy;

    @BeforeClass(groups = "deployment.import.group1")
    public void setup() throws Exception {
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);
        flags.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), false);
        flags.put(LatticeFeatureFlag.ENABLE_IMPORT_ERASE_BY_NULL.getName(), true);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG, flags);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment.import.group1")
    public void testEraseByNull() {
        SourceFile accountFile = uploadSourceFile(ACCOUNT_SOURCE_FILE_WITH_NULL, ENTITY_ACCOUNT);
        Assert.assertNotNull(accountFile);
        startCDLImport(accountFile, ENTITY_ACCOUNT, DEFAULT_SYSTEM);
        accountDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT), ENTITY_ACCOUNT);
        Table sourceTable = metadataProxy.getTable(customerSpace, accountFile.getTableName());
        Table accountTemplate = accountDataFeedTask.getImportTemplate();
        Assert.assertEquals(accountTemplate.getAttributes().size(), sourceTable.getAttributes().size());

        try {
            String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts", PathBuilder
                    .buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(mainTestTenant.getId()))
                    .toString(), SourceType.FILE.getName());
            Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, targetPath));
            String avroFileName = accountFile.getName().substring(0, accountFile.getName().lastIndexOf("."));
            List<String> avroFiles = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, targetPath,
                    file -> !file.isDirectory() && file.getPath().toString().contains(avroFileName)
                            && file.getPath().getName().endsWith("avro"));
            Assert.assertEquals(avroFiles.size(), 1);
            String avroFilePath = avroFiles.get(0).substring(0, avroFiles.get(0).lastIndexOf("/")) + "/*.avro";
            long rowCount = AvroUtils.count(yarnConfiguration, avroFilePath);
            Assert.assertEquals(rowCount, 25);
            Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroFiles.get(0)));
            Assert.assertEquals(schema.getFields().size(), 14);

            // test erase column value
            AvroUtils.AvroFilesIterator iterator = AvroUtils.iterateAvroFiles(yarnConfiguration, avroFilePath);
            int eraseLineCount = 0;
            Set<String> nullIdSet = new HashSet<>(Arrays.asList("0012400001DOBLuAAP", "0012400001DNcl1AAD"));
            while (iterator.hasNext()) {
                GenericRecord record = iterator.next();
                if (nullIdSet.contains(record.get(InterfaceName.CustomerAccountId.name()).toString())) {
                    Assert.assertNull(record.get(InterfaceName.Website.name()));
                    Assert.assertEquals(record.get("Erase_" + InterfaceName.Website).toString(), "true");
                    eraseLineCount++;
                } else {
                    Assert.assertNull(record.get("Erase_" + InterfaceName.Website));
                }
            }
            Assert.assertEquals(eraseLineCount, nullIdSet.size());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
