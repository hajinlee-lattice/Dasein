package com.latticeengines.apps.cdl.end2end;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class CsvImportEnd2EndDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CsvImportEnd2EndDeploymentTestNG.class);

    private static final boolean isEntityMatchMode = true;

    private String localDir = "datafeed";
    private String downloadDir = localDir + "/download";
    private String uploadDir = localDir + "/upload";

    @BeforeClass(groups = { "manual" })
    public void setup() throws Exception {
        if (isEntityMatchMode) {
            Map<String, Boolean> featureFlagMap = new HashMap<>();
            featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
            setupEnd2EndTestEnvironment(featureFlagMap);
        } else {
            setupEnd2EndTestEnvironment();
        }
//        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = "manual")
    public void runTest() throws Exception {
        FileUtils.deleteQuietly(new File(downloadDir));
        FileUtils.forceMkdirParent(new File(downloadDir));
        FileUtils.deleteQuietly(new File(uploadDir));
        FileUtils.forceMkdirParent(new File(uploadDir));
//        importAndDownload(BusinessEntity.Account);
//        clearHdfs();
        importAndDownload(BusinessEntity.Contact);
//        clearHdfs();
//        importAndDownload(BusinessEntity.Product);
//        clearHdfs();
//        importAndDownload(BusinessEntity.Transaction);
    }

    private void importAndDownload(BusinessEntity importingEntity) throws IOException {
        if (isEntityMatchMode) {
            importEntityMatchData(importingEntity);
        } else {
            importLegacyData(importingEntity);
        }
        downloadData();
        collectAvroFilesForEntity(importingEntity);
        saveImportTemplate(importingEntity);
    }

    private void importLegacyData(BusinessEntity importingEntity) {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());

        if (importingEntity.equals(BusinessEntity.Account)) {
            importData(BusinessEntity.Account, "Account_1_900.csv", "DefaultSystem_AccountData");
            importData(BusinessEntity.Account, "Account_401_500.csv", "DefaultSystem_AccountData");
            importData(BusinessEntity.Account, "Account_901_1000.csv", "DefaultSystem_AccountData");
        }

        if (importingEntity.equals(BusinessEntity.Contact)) {
            importData(BusinessEntity.Contact, "Contact_1_900.csv", "DefaultSystem_ContactData");
            importData(BusinessEntity.Contact, "Contact_401_500.csv", "DefaultSystem_ContactData");
            importData(BusinessEntity.Contact, "Contact_901_1000.csv", "DefaultSystem_ContactData");
        }

        if (importingEntity.equals(BusinessEntity.Product)) {
            importData(BusinessEntity.Product, "ProductBundles.csv", "DefaultSystem_ProductBundle");
            importData(BusinessEntity.Product, "ProductHierarchies.csv", "DefaultSystem_ProductHierarchy");
            importData(BusinessEntity.Product, "ProductBundle_MissingProductBundle.csv", "DefaultSystem_ProductBundle");
            importData(BusinessEntity.Product, "ProductHierarchies_MissingCategory.csv", "DefaultSystem_ProductHierarchy");
            importData(BusinessEntity.Product, "ProductHierarchies_MissingFamily.csv", "DefaultSystem_ProductHierarchy");
            // may need to update feed type for VDB import.
            importData(BusinessEntity.Product, "ProductVDB.csv", "ProductVDB");
        }

        if (importingEntity.equals(BusinessEntity.Transaction)) {
            importData(BusinessEntity.Transaction, "Transaction_1_25K.csv", "DefaultSystem_TransactionData");
            importData(BusinessEntity.Transaction, "Transaction_25K_50K.csv", "DefaultSystem_TransactionData");
            importData(BusinessEntity.Transaction, "Transaction_46K_60K.csv", "DefaultSystem_TransactionData");
        }
    }

    private void importEntityMatchData(BusinessEntity importingEntity) {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());

        if (importingEntity.equals(BusinessEntity.Account)) {
            importData(BusinessEntity.Account, "EntityMatch_Account_1_900.csv", "DefaultSystem_AccountData");
            importData(BusinessEntity.Account, "EntityMatch_Account_401_500.csv", "DefaultSystem_AccountData");
            importData(BusinessEntity.Account, "EntityMatch_Account_901_1000.csv", "DefaultSystem_AccountData");
        }

        if (importingEntity.equals(BusinessEntity.Contact)) {
            importData(BusinessEntity.Contact, "EntityMatch_Contact_1_900.csv", "DefaultSystem_ContactData");
            importData(BusinessEntity.Contact, "EntityMatch_Contact_401_500.csv", "DefaultSystem_ContactData");
            importData(BusinessEntity.Contact, "EntityMatch_Contact_901_1000.csv", "DefaultSystem_ContactData");
        }

        if (importingEntity.equals(BusinessEntity.Product)) {
            importData(BusinessEntity.Product, "ProductBundles.csv", "DefaultSystem_ProductBundle");
            importData(BusinessEntity.Product, "ProductHierarchies.csv", "DefaultSystem_ProductHierarchy");
            importData(BusinessEntity.Product, "ProductBundle_MissingProductBundle.csv", "DefaultSystem_ProductBundle");
            importData(BusinessEntity.Product, "ProductHierarchies_MissingCategory.csv", "DefaultSystem_ProductHierarchy");
            importData(BusinessEntity.Product, "ProductHierarchies_MissingFamily.csv", "DefaultSystem_ProductHierarchy");
            // may need to update feed type for VDB import.
            importData(BusinessEntity.Product, "ProductVDB.csv", "ProductVDB");
        }

        if (importingEntity.equals(BusinessEntity.Transaction)) {
            importData(BusinessEntity.Transaction, "Transaction_1_25K.csv", "DefaultSystem_TransactionData");
            importData(BusinessEntity.Transaction, "Transaction_25K_50K.csv", "DefaultSystem_TransactionData");
            importData(BusinessEntity.Transaction, "Transaction_46K_60K.csv", "DefaultSystem_TransactionData");
        }
    }

    private void downloadData() throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String dataFeedDir = String.format("%s/%s/DataFeed1",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.FILE.getName());
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, dataFeedDir, downloadDir);
        Collection<File> crcFiles = FileUtils.listFiles(new File(downloadDir), new String[] { "crc" }, true);
        crcFiles.forEach(FileUtils::deleteQuietly);
    }

    private void clearHdfs() throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String dataFeedDir = String.format("%s/%s/DataFeed1",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.FILE.getName());
        HdfsUtils.rmdir(yarnConfiguration, dataFeedDir);
        FileUtils.deleteDirectory(new File(downloadDir));
    }

    private void collectAvroFilesForEntity(BusinessEntity entity) throws IOException {
        File rootDir = new File(downloadDir + String.format("/DataFeed1-%s/Extracts", entity.name()));
        if (rootDir.exists()) {
            List<File> files = new ArrayList<>(FileUtils.listFiles(rootDir, new String[] { "avro" }, true));
            files.sort(Comparator.comparing(File::getPath));
            for (int i = 0; i < files.size(); i++) {
                File src = files.get(i);
                File tgt = new File(String.format("%s/%s-%d.avro", uploadDir, entity.name(), i + 1));
                FileUtils.copyFile(src, tgt);
            }
            log.info("Copied " + files.size() + " " + entity + " extracts to upload folder.");
        } else {
            log.info("No " + entity + " extracts to be copied.");
        }
    }

    private void saveImportTemplate(BusinessEntity entity) throws IOException {
        if (BusinessEntity.Product.equals(entity)) {
            saveImportTemplate(entity, "DefaultSystem_ProductBundle");
            saveImportTemplate(entity, "DefaultSystem_ProductHierarchy");
            saveImportTemplate(entity, "ProductVDB");
        } else {
            saveImportTemplate(entity, "DefaultSystem_" + entity.name() + "Data");
        }
    }

    private void saveImportTemplate(BusinessEntity entity, String feedType) throws IOException {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SourceType.FILE.getName(),
                feedType, entity.name());
        if (dataFeedTask != null) {
            Table importTemplate = dataFeedTask.getImportTemplate();
            String fileName;
            if (isEntityMatchMode) {
                fileName = entity + "_" + ADVANCED_MATCH_SUFFIX + "_" + feedType + ".json";
            } else {
                fileName = entity + "_" + feedType + ".json";
            }
            File jsonFile = new File(uploadDir + "/" + fileName);
            FileUtils.touch(jsonFile);
            JsonUtils.serialize(importTemplate, new FileOutputStream(jsonFile));
            log.info("Saved " + entity + " template to upload folder");
        } else {
            log.info("No data feed task for entity " + entity + " of type " + feedType);
        }
    }

}
