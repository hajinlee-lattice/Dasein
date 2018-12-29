package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.VdbImportConfig;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class DataFeedTaskManagerServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private VdbImportConfig vdbMetadata1;

    private VdbImportConfig vdbMetadata2;
    private static final String SOURCE = "VisiDB";
    private static final String ENTITY_ACCOUNT = "Account";
    private static final String FEED_TYPE_SUFFIX = "Schema";
    @Inject
    private DataFeedTaskManagerService dataFeedTaskManagerService;

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @Inject
    private ActionService actionService;

    @BeforeClass(groups = "deployment")
    public void setup() throws IOException {
        setupTestEnvironment();
        vdbMetadata1 = new VdbImportConfig();
        vdbMetadata1.setVdbLoadTableConfig(JsonUtils.deserialize(
                IOUtils.toString(Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("metadata/vdb/test1.json"), "UTF-8"),
                VdbLoadTableConfig.class));
        vdbMetadata1.getVdbLoadTableConfig().setTenantId(mainTestTenant.getId());
        vdbMetadata2 = new VdbImportConfig();
        vdbMetadata2.setVdbLoadTableConfig(JsonUtils.deserialize(
                IOUtils.toString(Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("metadata/vdb/test1.json"), "UTF-8"),
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
    }

    @Test(groups = "deployment")
    public void testCreateDataFeedTask() {
        String taskId1 = dataFeedTaskManagerService.createDataFeedTask(mainTestTenant.getId(),
                ENTITY_ACCOUNT + FEED_TYPE_SUFFIX, ENTITY_ACCOUNT,
                "VisiDB", "", "", false, "", vdbMetadata1);
        Assert.assertNotNull(taskId1);
        String taskId2 = dataFeedTaskManagerService.createDataFeedTask(mainTestTenant.getId(),
                ENTITY_ACCOUNT + FEED_TYPE_SUFFIX, ENTITY_ACCOUNT,
                "VisiDB", "", "", false, "", vdbMetadata2);
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

}
