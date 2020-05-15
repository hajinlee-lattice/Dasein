package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.VdbImportConfig;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskConfig;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.util.DataFeedTaskUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

public class DataFeedTaskManagerServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private VdbImportConfig vdbMetadata1;

    private VdbImportConfig vdbMetadata2;
    private static final String SOURCE = "VisiDB";
    private static final String ENTITY_ACCOUNT = "Account";
    private static final String FEED_TYPE_SUFFIX = "Schema";
    private static final String FEED_TYPE = "DefaultSystem_AccountData";
    private static final String TEST_DATA_DIR = "le-serviceapps/cdl/end2end/csv";
    private static final String TEST_DATA_VERSION = "6";
    private static final String TEST_ACCOUNT_DATA_FILE = "Account_1_900.csv";

    @Inject
    private DataFeedTaskManagerService dataFeedTaskManagerService;

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @Inject
    private ActionService actionService;

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    protected TestArtifactService testArtifactService;

    @Inject
    private Configuration yarnConfiguration;

    @BeforeClass(groups = "deployment")
    public void setup() throws IOException {
        setupTestEnvironment();
        vdbMetadata1 = new VdbImportConfig();
        vdbMetadata1.setVdbLoadTableConfig(JsonUtils.deserialize(
                IOUtils.toString(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("metadata/vdb/test1.json")), "UTF-8"),
                VdbLoadTableConfig.class));
        vdbMetadata1.getVdbLoadTableConfig().setTenantId(mainTestTenant.getId());
        vdbMetadata2 = new VdbImportConfig();
        vdbMetadata2.setVdbLoadTableConfig(JsonUtils.deserialize(
                IOUtils.toString(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("metadata/vdb/test1.json")), "UTF-8"),
                VdbLoadTableConfig.class));
        vdbMetadata2.getVdbLoadTableConfig().setTenantId(mainTestTenant.getId());
        vdbMetadata1.getVdbLoadTableConfig().getMetadataList().forEach(columnMetadata -> {
            if (columnMetadata.getColumnName().equalsIgnoreCase("Country")) {
                columnMetadata.setApprovedUsage(new ArrayList<>());
                columnMetadata.getApprovedUsage().add("Model");
                columnMetadata.setTags(new ArrayList<>());
                columnMetadata.getTags().add("ExcludeFromAll");
            }
        });
        vdbMetadata2.getVdbLoadTableConfig().getMetadataList().forEach(columnMetadata -> {
            if (columnMetadata.getColumnName().equalsIgnoreCase("Country")) {
                columnMetadata.setApprovedUsage(new ArrayList<>());
                columnMetadata.getApprovedUsage().add("Model");
                columnMetadata.setTags(new ArrayList<>());
                columnMetadata.getTags().add("ExcludeFromListView");
            }
            if (columnMetadata.getColumnName().equalsIgnoreCase("Postal_Code")) {
                columnMetadata.setApprovedUsage(new ArrayList<>());
                columnMetadata.getApprovedUsage().add("ModelAndAllInsights");
                columnMetadata.setTags(new ArrayList<>());
                columnMetadata.getTags().add("ExcludeFromListView");
            }
        });
        vdbMetadata2.getVdbLoadTableConfig().getMetadataList().remove(5);
    }

    @Test(groups = "deployment")
    public void testCreateDataFeedTask() {
        String taskId1 = dataFeedTaskManagerService.createDataFeedTask(mainTestTenant.getId(),
                ENTITY_ACCOUNT + FEED_TYPE_SUFFIX, ENTITY_ACCOUNT,
                "VisiDB", "", "", false, "1", vdbMetadata1);
        Assert.assertNotNull(taskId1);
        String taskId2 = dataFeedTaskManagerService.createDataFeedTask(mainTestTenant.getId(),
                ENTITY_ACCOUNT + FEED_TYPE_SUFFIX, ENTITY_ACCOUNT,
                "VisiDB", "", "", false, "2", vdbMetadata2);

        Assert.assertEquals(taskId1, taskId2);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateDataFeedTask", enabled = false)
    public void testImportVDB() {
        DataFeedTask task = dataFeedTaskService.getDataFeedTask(mainCustomerSpace, SOURCE,
                ENTITY_ACCOUNT + FEED_TYPE_SUFFIX,
                ENTITY_ACCOUNT);
        String applicationId = dataFeedTaskManagerService.submitImportJob(mainCustomerSpace, task.getUniqueId(),
                vdbMetadata1);
        JobStatus status = waitForWorkflowStatus(applicationId, false);
        Assert.assertEquals(status, JobStatus.COMPLETED);

    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateDataFeedTask")
    public void testResetImport() {
        dataFeedTaskManagerService.resetImport(mainCustomerSpace, BusinessEntity.Account);
        List<DataFeedTask> tasks = dataFeedTaskService.getDataFeedTaskWithSameEntity(mainCustomerSpace, ENTITY_ACCOUNT);

        // verify delete the template table
        for (DataFeedTask task : tasks) {
            Assert.assertNull(task.getImportTemplate());
        }
        //verify action is deleted 
        Set<String> taskIds = tasks.stream().map(DataFeedTask::getUniqueId).collect(Collectors.toSet());
        List<Action> actions = actionService.findAll().stream()
                .filter(action -> action.getType().equals(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(actions)) {
            for (Action action : actions) {
                if (action.getActionConfiguration() != null
                        && action.getActionConfiguration() instanceof ImportActionConfiguration) {
                    ImportActionConfiguration actionConfig = (ImportActionConfiguration) action.getActionConfiguration();
                    Assert.assertEquals(false, taskIds.contains(actionConfig.getDataFeedTaskId()));
                }
            }
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "testResetImport")
    public void testDataFeedTaskConfig() throws IOException {
        // create source file
        InputStream stream = testArtifactService.readTestArtifactAsStream(TEST_DATA_DIR, TEST_DATA_VERSION,
                TEST_ACCOUNT_DATA_FILE);

        CustomerSpace space = CustomerSpace.parse(mainCustomerSpace);
        String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), space).toString();
        SourceFile file = new SourceFile();
        file.setTenant(mainTestTenant);
        file.setName(TEST_ACCOUNT_DATA_FILE);
        file.setPath(outputPath + "/" + TEST_ACCOUNT_DATA_FILE);
        file.setSchemaInterpretation(SchemaInterpretation.Account);
        file.setBusinessEntity(BusinessEntity.Account);
        file.setState(SourceFileState.Uploaded);
        file.setDisplayName(TEST_ACCOUNT_DATA_FILE);

        long rows = saveFileToHdfs(TEST_ACCOUNT_DATA_FILE, stream, outputPath);
        file.setFileRows(rows);
        sourceFileProxy.create(mainCustomerSpace, file);

        // set up template
        Table templateTable = SchemaRepository.instance().getSchema(S3ImportSystem.SystemType.Other,
                EntityType.Accounts, true);
        templateTable.getAttributes().forEach(attribute -> {
            if (InterfaceName.CustomerAccountId.equals(attribute.getInterfaceName())) {
                attribute.setSourceAttrName("Id");
            }
            if (InterfaceName.CompanyName.equals(attribute.getInterfaceName())) {
                attribute.setSourceAttrName("Company Name");
            }
        });
        templateTable.setName(NamingUtils.uuid("AccountTemplate"));
        metadataProxy.createImportTable(mainCustomerSpace, templateTable.getName(), templateTable);
        DataFeedTask task = DataFeedTaskUtils.generateDataFeedTask(FEED_TYPE, "File", templateTable,
                EntityType.Accounts, "AccountData");
        DataFeedTaskConfig config = new DataFeedTaskConfig();
        config.setLimitPerImport(100L);
        task.setDataFeedTaskConfig(config);
        dataFeedTaskService.createDataFeedTask(mainCustomerSpace, task);

        // generate import config
        CSVToHdfsConfiguration importConfig = new CSVToHdfsConfiguration();
        SourceFile dataSourceFile = sourceFileProxy.findByName(mainCustomerSpace, file.getName());

        importConfig.setCustomerSpace(CustomerSpace.parse(mainCustomerSpace));
        importConfig.setTemplateName(task.getImportTemplate().getName());
        importConfig.setFilePath(dataSourceFile.getPath());
        importConfig.setFileSource("HDFS");
        CSVImportFileInfo importFileInfo = new CSVImportFileInfo();
        importFileInfo.setFileUploadInitiator("test@dnb.com");
        importFileInfo.setReportFileDisplayName(dataSourceFile.getDisplayName());
        importFileInfo.setReportFileName(dataSourceFile.getName());
        CSVImportConfig csvImportConfig = new CSVImportConfig();
        csvImportConfig.setCsvToHdfsConfiguration(importConfig);
        csvImportConfig.setCSVImportFileInfo(importFileInfo);
        String appId = dataFeedTaskManagerService.submitDataOnlyImportJob(mainCustomerSpace, task.getUniqueId(),
                csvImportConfig);
        JobStatus status = waitForWorkflowStatus(appId, false);
        Assert.assertEquals(status, JobStatus.FAILED);
        Job importJob = getWorkflowJobFromApplicationId(appId);
        Assert.assertEquals(importJob.getErrorCode(), LedpCode.LEDP_40083);
    }

    private long saveFileToHdfs(String outputFileName, InputStream inputStream, String outputPath) throws IOException {
        return HdfsUtils.copyInputStreamToHdfsWithoutBomAndReturnRows(yarnConfiguration, inputStream,
                outputPath + "/" + outputFileName);
    }

}
