package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.proxy.TestDataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class DataIngestionEnd2EndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(DataIngestionEnd2EndDeploymentTestNG.class);

    private static final String DATA_COLLECTION_NAME = "DATA_COLLECTION_NAME";

    private static final String DATA_FEED_NAME = "DATA_FEED_NAME";

    private static final String DL_TENANT_NAME = "ManualELQ_2016_1215_1051";

    private static final String DL_LOAD_GROUP = "TestDP_20170615";

    private static final String DL_ENDPOINT = "http://10.41.1.187:8089";

    private static final String COLLECTION_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss";

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private TestDataFeedProxy dataFeedProxy;

    @Autowired
    protected Configuration yarnConfiguration;

    @Value("${common.test.env}")
    private String testEnv;

    private Tenant firstTenant;

    @BeforeClass(groups = { "deployment.cdl" })
    public void setup() throws Exception {
        log.info("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        Tenant tenant = testBed.addExtraTestTenant(CustomerSpace.parse(DL_TENANT_NAME).toString());
        testBed.deleteTenant(testBed.getMainTestTenant());
        testBed.setMainTestTenant(tenant);
        testBed.switchToSuperAdmin();
        firstTenant = testBed.getMainTestTenant();
        Tenant retrieved = testBed.getMainTestTenant();
        Assert.assertEquals(retrieved.getId(), CustomerSpace.parse(DL_TENANT_NAME).toString());

        attachProtectedProxy(dataFeedProxy);
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(firstTenant));

        log.info("Test environment setup finished.");
        createDataFeed();
    }

    @Test(groups = { "deployment.cdl" })
    public void importData() throws Exception {
        if (testEnv.equalsIgnoreCase("dev")) {
            mockAvroData();
            Thread.sleep(5000);
            mockAvroData();
        } else {
            String launchId = startExecuteGroup();
            long startMillis = System.currentTimeMillis();
            waitLoadGroup(launchId, 1800);
            long endMillis = System.currentTimeMillis();
            checkExtractFolderExist(startMillis, endMillis);
        }
        metadataProxy.updateDataFeedStatus(firstTenant.getId(), DATA_FEED_NAME, Status.InitialLoaded.getName());
    }

    @Test(groups = { "deployment.cdl" }, dependsOnMethods = "importData")
    public void initialConsolidate() {
        log.info("Start consolidating data ...");
        ApplicationId appId = dataFeedProxy.consolidate(DATA_FEED_NAME);
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = { "deployment.cdl" }, dependsOnMethods = "initialConsolidate")
    public void firstFinalize() {
        log.info("Start calculating statistics ...");
        ApplicationId appId = dataFeedProxy.assemble(DATA_FEED_NAME);
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = { "deployment.cdl" }, dependsOnMethods = "firstFinalize")
    public void verifyFirstImport() throws IOException {
        String customerSpace = CustomerSpace.parse(firstTenant.getId()).toString();
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStatsInDefaultColellction(customerSpace);
        Assert.assertNotNull(statisticsContainer);
        // save stats to a local json to help create verifications
        File statsJson = new File("stats.json");
        FileUtils.deleteQuietly(statsJson);
        FileUtils.write(statsJson, JsonUtils.pprint(statisticsContainer));
    }

    @Test(groups = { "deployment.cdl" }, dependsOnMethods = "verifyFirstImport")
    public void importSecondData() {
        log.info("Start second consolidating data ...");
        ApplicationId appId = dataFeedProxy.consolidate(DATA_FEED_NAME);
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = { "deployment.cdl" }, dependsOnMethods = "importSecondData")
    public void secondConsolidate() {

    }

    @Test(groups = { "deployment.cdl" }, dependsOnMethods = "secondConsolidate")
    public void secondFinalize() {

    }

    @Test(groups = { "deployment.cdl" }, dependsOnMethods = "secondFinalize")
    public void verifySecondImport() {

    }

    @Test(groups = { "deployment.cdl" }, dependsOnMethods = "verifySecondImport")
    public void querySegment() {

    }

    private void mockAvroData() throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(DL_TENANT_NAME);
        URL dataUrl = ClassLoader.getSystemResource("com/latticeengines/pls/end2end/cdl/Extract_Accounts_0.avro");
        Table importTemplate = null;

        // DataFeedTask dataFeedTask = new DataFeedTask();
        DataFeedTask dataFeedTask = metadataProxy.getDataFeedTask(customerSpace.toString(), "VisiDB", "Query",
                "Account", DATA_FEED_NAME);
        if (dataFeedTask == null) {
            Schema schema = AvroUtils.readSchemaFromLocalFile(dataUrl.getPath());
            importTemplate = MetadataConverter.getTable(schema, new ArrayList<>(), null, null, false);
            importTemplate.setTableType(TableType.IMPORTTABLE);
            importTemplate.setName(SchemaInterpretation.Account.name());
            dataFeedTask = new DataFeedTask();
            dataFeedTask.setImportTemplate(importTemplate);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity("Account");
            dataFeedTask.setFeedType("Query");
            dataFeedTask.setSource("VisiDB");
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));

            metadataProxy.createDataFeedTask(customerSpace.toString(), DATA_FEED_NAME, dataFeedTask);
        } else {
            importTemplate = dataFeedTask.getImportTemplate();
        }

        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.VISIDB.getName(), new SimpleDateFormat(COLLECTION_DATE_FORMAT).format(new Date()));
        String fileName = dataUrl.getPath().substring(dataUrl.getPath().lastIndexOf("/") + 1);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, dataUrl.getPath(),
                String.format("%s/%s", targetPath, fileName));

        String defaultFS = yarnConfiguration.get(FileSystem.FS_DEFAULT_NAME_KEY);
        String hdfsUri = String.format("%s%s/%s", defaultFS, targetPath, "*.avro");
        Extract e = createExtract(hdfsUri, 1000L);

        dataFeedTask = metadataProxy.getDataFeedTask(customerSpace.toString(), "VisiDB", "Query", "Account",
                DATA_FEED_NAME);

        metadataProxy.registerExtract(customerSpace.toString(), dataFeedTask.getPid().toString(),
                importTemplate.getName(), e);
    }

    private Extract createExtract(String path, long processedRecords) {
        Extract e = new Extract();
        e.setName(StringUtils.substringAfterLast(path, "/"));
        e.setPath(PathUtils.stripoutProtocol(path));
        e.setProcessedRecords(processedRecords);
        String dateTime = StringUtils.substringBetween(path, "/Extracts/", "/");
        SimpleDateFormat f = new SimpleDateFormat(COLLECTION_DATE_FORMAT);
        try {
            e.setExtractionTimestamp(f.parse(dateTime).getTime());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return e;
    }

    private void checkExtractFolderExist(long startMillis, long endMillis) throws Exception {
        CustomerSpace customerSpace = CustomerSpace.parse(DL_TENANT_NAME);
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.VISIDB.getName());
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, targetPath));
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, targetPath);
        for (String file : files) {
            String filename = file.substring(file.lastIndexOf("/") + 1);
            Date folderTime = new SimpleDateFormat(COLLECTION_DATE_FORMAT).parse(filename);
            if (folderTime.getTime() > startMillis && folderTime.getTime() < endMillis) {
                return;
            }
            log.info("File name: " + filename);
        }
        assertTrue(false, "No data collection folder was created!");
    }

    private String startExecuteGroup() {
        String url = DL_ENDPOINT + "/DLRestService/ExecuteGroup";
        Map<String, String> body = new HashMap<>();
        body.put("email", "LP");
        body.put("tenantName", DL_TENANT_NAME);
        body.put("groupName", DL_LOAD_GROUP);
        body.put("state", "launch");
        String response = magicRestTemplate.postForObject(url, body, String.class);
        InstallResult result = JsonUtils.deserialize(response, InstallResult.class);
        String launchId = null;
        List<InstallResult.ValueResult> valueResults = result.getValueResult();
        if (valueResults != null) {
            for (InstallResult.ValueResult valueResult : valueResults) {
                if ("launchId".equalsIgnoreCase(valueResult.getKey())) {
                    launchId = valueResult.getValue();
                    break;
                }
            }
        }
        return launchId;
    }

    private void waitLoadGroup(String launchId, int maxSeconds) {
        String url = DL_ENDPOINT + "/DLRestService/GetLaunchStatus";
        Map<String, String> body = new HashMap<>();
        body.put("launchId", launchId);
        int i = 0;
        while (i < maxSeconds) {
            String response = magicRestTemplate.postForObject(url, body, String.class);
            InstallResult result = JsonUtils.deserialize(response, InstallResult.class);
            List<InstallResult.ValueResult> valueResults = result.getValueResult();
            if (valueResults != null) {
                String running = "true";
                for (InstallResult.ValueResult valueResult : valueResults) {
                    if ("Running".equalsIgnoreCase(valueResult.getKey())) {
                        running = valueResult.getValue();
                        break;
                    }
                }
                if (running.equalsIgnoreCase("false")) {
                    break;
                }
            }
            i++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private void createDataFeed() {
        DataCollection dataCollection = new DataCollection();
        dataCollection.setName(DATA_COLLECTION_NAME);
        dataCollection.setType(DataCollectionType.Segmentation);
        dataCollectionProxy.createOrUpdateDataCollection(firstTenant.getId(), dataCollection);

        DataCollection defaultCollectin = dataCollectionProxy.getDefaultDataCollection(firstTenant.getId());
        Assert.assertEquals(defaultCollectin.getName(), DATA_COLLECTION_NAME);

        DataFeed datafeed = new DataFeed();
        datafeed.setName(DATA_FEED_NAME);

        Table importTable = new Table();
        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(firstTenant);

        Table dataTable = new Table();
        dataTable.setName("dataTable");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(firstTenant);

        // DataFeedTask task = new DataFeedTask();
        // task.setDataFeed(datafeed);
        // task.setActiveJob("1");
        // task.setEntity(SchemaInterpretation.Account.name());
        // task.setSource("VDB");
        // task.setStatus(DataFeedTask.Status.Active);
        // task.setSourceConfig("config");
        // task.setImportTemplate(importTable);
        // task.setImportData(dataTable);
        // task.setStartTime(new Date());
        // task.setLastImported(new Date());
        // datafeed.addTask(task);
        dataCollectionProxy.addDataFeed(firstTenant.getId(), DATA_COLLECTION_NAME, datafeed);
    }
}
