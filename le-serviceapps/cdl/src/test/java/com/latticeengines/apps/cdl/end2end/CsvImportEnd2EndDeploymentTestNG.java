package com.latticeengines.apps.cdl.end2end;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

/*
 * dpltc deploy -a pls,admin,cdl,modeling,lp,metadata,workflowapi,eai,matchapi
 */
public class CsvImportEnd2EndDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CsvImportEnd2EndDeploymentTestNG.class);

    private static final String WEBSITE_SYSTEM = "Default_Website_System";
    private static final String OPPORTUNITY_SYSTEM = "Default_Opportunity_System";

    private static final boolean isEntityMatchMode = true;

    private String localDir = "datafeed";
    private String downloadDir = localDir + "/download";
    private String uploadDir = localDir + "/upload";

    @Inject
    private DropBoxProxy dropBoxProxy;

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

    @Test(groups = "manual", enabled = false)
    public void runTest() throws Exception {
        ensureEmptyDirs();
//        importAndDownload(BusinessEntity.Account);
//        clearHdfs();
        // importAndDownload(BusinessEntity.Contact);
        S3ImportSystem system = createSystem();
        cdlProxy.createDefaultOpportunityTemplate(mainCustomerSpace, system.getName());
        importAndDownload(BusinessEntity.Contact);
//        clearHdfs();
//        importAndDownload(BusinessEntity.Product);
//        clearHdfs();
//        importAndDownload(BusinessEntity.Transaction);
    }

    @Test(groups = "manual", enabled = false)
    private void importWebVisitData() throws Exception {
        ensureEmptyDirs();
        // setup web visit templates
        SimpleTemplateMetadata webVisit = new SimpleTemplateMetadata();
        webVisit.setEntityType(EntityType.WebVisit);
        Set<String> ignoredAttrSet = Sets.newHashSet(InterfaceName.Website.name(), InterfaceName.PostalCode.name());
        webVisit.setIgnoredStandardAttributes(ignoredAttrSet);
        cdlProxy.createWebVisitTemplate2(mainCustomerSpace, Collections.singletonList(webVisit));
        SimpleTemplateMetadata ptn = new SimpleTemplateMetadata();
        ptn.setEntityType(EntityType.WebVisitPathPattern);
        cdlProxy.createWebVisitTemplate2(mainCustomerSpace, Collections.singletonList(ptn));
        SimpleTemplateMetadata sm = new SimpleTemplateMetadata();
        sm.setEntityType(EntityType.WebVisitSourceMedium);
        cdlProxy.createWebVisitTemplate2(mainCustomerSpace, Collections.singletonList(sm));

        // import opportunity & stage data
        importData(BusinessEntity.ActivityStream, "webVisit_no_sm.csv",
                EntityTypeUtils.generateFullFeedType(WEBSITE_SYSTEM, EntityType.WebVisit), false, false);
        // importData(BusinessEntity.Catalog, "webVisitPathPtn.csv",
        // EntityTypeUtils.generateFullFeedType(WEBSITE_SYSTEM,
        // EntityType.WebVisitPathPattern), false, false);
        // importData(BusinessEntity.Catalog, "webVisitSrcMedium.csv",
        // EntityTypeUtils.generateFullFeedType(WEBSITE_SYSTEM,
        // EntityType.WebVisitSourceMedium), false, false);

        downloadData();
        collectAvroFilesForEntity(BusinessEntity.ActivityStream);
        // collectAvroFilesForEntity(BusinessEntity.Catalog);
    }

    @Test(groups = "manual", enabled = false)
    private void importOpportunityData() throws Exception {
        ensureEmptyDirs();

        S3ImportSystem system = createSystem();
        dropBoxProxy.createTemplateFolder(mainCustomerSpace, system.getName(), null, null);
        cdlProxy.createDefaultOpportunityTemplate(mainCustomerSpace, system.getName());

        // import opportunity & stage data
        importData(BusinessEntity.ActivityStream, "opportunity_1ec2c252_f36d_4054_aaf4_9d6379006244.csv",
                EntityTypeUtils.generateFullFeedType(system.getName(), EntityType.Opportunity), false, false);
        importData(BusinessEntity.Catalog, "opportunity_stage.csv",
                EntityTypeUtils.generateFullFeedType(system.getName(), EntityType.OpportunityStageName), false, false);

        // download data (template not required, just use the API created ones in e2e)
        downloadData();
        collectAvroFilesForEntity(BusinessEntity.ActivityStream);
        collectAvroFilesForEntity(BusinessEntity.Catalog);
    }

    @Test(groups = "manual", enabled = false)
    private void importMarketingData() throws Exception {
        ensureEmptyDirs();
        String systemName = "Default_Marketo_System";
        S3ImportSystem system = new S3ImportSystem();
        system.setTenant(mainTestTenant);
        system.setName(systemName);
        system.setDisplayName(systemName);
        system.setSystemType(S3ImportSystem.SystemType.Marketo);
        system.setPriority(2);
        // dummy id, maybe just use the customer account id
        system.setContactSystemId(String.format("user_%s_dlugenoz_ContactId", systemName));
        system.setMapToLatticeContact(true);
        cdlProxy.createS3ImportSystem(mainCustomerSpace, system);
        dropBoxProxy.createTemplateFolder(mainCustomerSpace, systemName, null, null);

        cdlProxy.createDefaultMarketingTemplate(mainCustomerSpace, system.getName(), S3ImportSystem.SystemType.Marketo.name());

        importData(BusinessEntity.ActivityStream, "marketing_8e1c6639_6eb6_40ee_ad58_0e86a192a1c7.csv",
                EntityTypeUtils.generateFullFeedType(system.getName(), EntityType.MarketingActivity), false, false);
        importData(BusinessEntity.Catalog, "marketing_catalog_c2f17396_45e5_4a75_bb60_75fccaec07ed.csv",
                EntityTypeUtils.generateFullFeedType(system.getName(), EntityType.MarketingActivityType), false, false);

        downloadData();
        collectAvroFilesForEntity(BusinessEntity.ActivityStream);
        collectAvroFilesForEntity(BusinessEntity.Catalog);
    }

    @Test(groups = "manual")
    private void importIntentActivityData() throws Exception {
        ensureEmptyDirs();
        String systemName = S3ImportSystem.SystemType.Other.getDefaultSystemName();
        S3ImportSystem system = new S3ImportSystem();
        system.setTenant(mainTestTenant);
        system.setName(systemName);
        system.setDisplayName(systemName);
        system.setSystemType(S3ImportSystem.SystemType.Other);
        system.setPriority(2);
        cdlProxy.createS3ImportSystem(mainCustomerSpace, system);
        dropBoxProxy.createTemplateFolder(mainCustomerSpace, systemName, null, null);

        cdlProxy.createDefaultDnbIntentDataTemplate(mainCustomerSpace);
        log.info("Finished creating default Intent Data template.");

        String templateFeedType = EntityTypeUtils.generateFullFeedType(systemName, EntityType.CustomIntent);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(mainCustomerSpace, "File", templateFeedType);

        importOnlyDataFromS3(BusinessEntity.ActivityStream, "intent_activity_data.csv", dataFeedTask);
        downloadData();
        collectAvroFilesForEntity(BusinessEntity.ActivityStream);
    }

    private void ensureEmptyDirs() throws Exception {
        FileUtils.deleteQuietly(new File(downloadDir));
        FileUtils.forceMkdirParent(new File(downloadDir));
        FileUtils.deleteQuietly(new File(uploadDir));
        FileUtils.forceMkdirParent(new File(uploadDir));
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
            importData(BusinessEntity.Account, "Account_1_900.csv");
            importData(BusinessEntity.Account, "Account_401_500.csv");
            importData(BusinessEntity.Account, "Account_901_1000.csv");
        }

        if (importingEntity.equals(BusinessEntity.Contact)) {
            importData(BusinessEntity.Contact, "Contact_1_900.csv");
            importData(BusinessEntity.Contact, "Contact_401_500.csv");
            importData(BusinessEntity.Contact, "Contact_901_1000.csv");
        }

        if (importingEntity.equals(BusinessEntity.Product)) {
            importData(BusinessEntity.Product, "ProductBundles.csv");
            importData(BusinessEntity.Product, "ProductHierarchies.csv");
            importData(BusinessEntity.Product, "ProductBundle_MissingProductBundle.csv");
            importData(BusinessEntity.Product, "ProductHierarchies_MissingCategory.csv");
            importData(BusinessEntity.Product, "ProductHierarchies_MissingFamily.csv");
            // may need to update feed type for VDB import.
            importData(BusinessEntity.Product, "ProductVDB.csv");
        }

        if (importingEntity.equals(BusinessEntity.Transaction)) {
            importData(BusinessEntity.Transaction, "Transaction_1_25K.csv");
            importData(BusinessEntity.Transaction, "Transaction_25K_50K.csv");
            importData(BusinessEntity.Transaction, "Transaction_46K_60K.csv");
        }
    }

    private void importEntityMatchData(BusinessEntity importingEntity) {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());

        if (importingEntity.equals(BusinessEntity.Account)) {
            importData(BusinessEntity.Account, "EntityMatch_Account_1_900.csv");
            importData(BusinessEntity.Account, "EntityMatch_Account_401_500.csv");
            importData(BusinessEntity.Account, "EntityMatch_Account_901_1000.csv");
        }

        if (importingEntity.equals(BusinessEntity.Contact)) {
            importData(BusinessEntity.Contact, "EntityMatch_Contact_1_900.csv");
            importData(BusinessEntity.Contact, "EntityMatch_Contact_401_500.csv");
            importData(BusinessEntity.Contact, "EntityMatch_Contact_901_1000.csv");
        }

        if (importingEntity.equals(BusinessEntity.Product)) {
            importData(BusinessEntity.Product, "ProductBundles.csv");
            importData(BusinessEntity.Product, "ProductHierarchies.csv");
            importData(BusinessEntity.Product, "ProductBundle_MissingProductBundle.csv");
            importData(BusinessEntity.Product, "ProductHierarchies_MissingCategory.csv");
            importData(BusinessEntity.Product, "ProductHierarchies_MissingFamily.csv");
            // may need to update feed type for VDB import.
            importData(BusinessEntity.Product, "ProductVDB.csv");
        }

        if (importingEntity.equals(BusinessEntity.Transaction)) {
            importData(BusinessEntity.Transaction, "EntityMatch_Transaction_1_25K.csv");
            importData(BusinessEntity.Transaction, "EntityMatch_Transaction_25K_50K.csv");
            importData(BusinessEntity.Transaction, "EntityMatch_Transaction_46K_60K.csv");
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

    /*-
     * create a dummy system for opportunity templates to attach to
     */
    private S3ImportSystem createSystem() {
        String systemName = OPPORTUNITY_SYSTEM;
        S3ImportSystem system = new S3ImportSystem();
        system.setTenant(mainTestTenant);
        system.setName(systemName);
        system.setDisplayName(systemName);
        system.setSystemType(S3ImportSystem.SystemType.Other);
        system.setPriority(1);
        system.setAccountSystemId(String.format("user_%s_dlugenoz_AccountId", systemName));
        system.setMapToLatticeAccount(true);
        cdlProxy.createS3ImportSystem(mainCustomerSpace, system);
        return system;
    }

}
