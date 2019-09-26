package com.latticeengines.apps.cdl.workflow;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.metadata.entitymgr.MigrationTrackEntityMgr;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class ProcessAnalyzeWorkflowSubmitterTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessAnalyzeWorkflowSubmitterTestNG.class);

    private static final String customerSpace = "tenant";
    private static final String lockedCustomerSpace = "LockedTenant";
    private static final String unlockedCustomerSpace = "UnlockedTenant";
    private static final Long METADATA_ACTION_PID = 1L;
    private static final Long RUNNING_ACTION_1_PID = 2L;
    private static final Long RUNNING_ACTION_2_PID = 3L;
    private static final Long COMPLETE_ACTION_1_PID = 4L;
    private static final Long COMPLETE_ACTION_2_PID = 5L;
    private static final Long PROBLEMATIC_ACTION_NO_TRACKING_ID_PID = 6L;
    private static final Long CANCEL_ACTION_1_PID = 7L;
    private static final Long CANCEL_ACTION_2_PID = 8L;
    private static final Long NEW_DELETE_ACTION_PID = 9L;
    private static final Long RUNNING_ACTION_1_TRACKING_PID = 101L;
    private static final Long RUNNING_ACTION_2_TRACKING_PID = 102L;
    private static final Long RUNNING_ACTION_1_TRACKING_ID = 1101L;
    private static final Long RUNNING_ACTION_2_TRACKING_ID = 1102L;
    private static final Long COMPLETE_ACTION_1_TRACKING_PID = 103L;
    private static final Long COMPLETE_ACTION_2_TRACKING_PID = 104L;
    private static final Long COMPLETE_ACTION_1_TRACKING_ID = 1103L;
    private static final Long COMPLETE_ACTION_2_TRACKING_ID = 1104L;
    private static final Long DEFAULT_WORKFLOW_ID = 200L;
    private static final Long DEFAULT_WORKFLOW_PID = 201L;

    @Mock
    private ActionService actionService;

    @Mock
    private DataFeedService dataFeedService;

    @Mock
    private DataFeedTaskService dataFeedTaskService;

    @Mock
    private WorkflowProxy workflowProxy;

    @Mock
    private MigrationTrackEntityMgr migrationTrackEntityMgr;

    @Mock
    private TenantEntityMgr tenantEntityMgr;

    @InjectMocks
    private ProcessAnalyzeWorkflowSubmitter processAnalyzeWorkflowSubmitter;

    @BeforeTest(groups = "functional")
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test(groups = "functional")
    public void testGetEmptyActionAndJobIds() {
        when(actionService.findByOwnerId(nullable(Long.class))).thenReturn(generateEmptyActions());
        List<Long> list = toActionPids(
                processAnalyzeWorkflowSubmitter.getCompletedActions(customerSpace, generateEmptyActions(), new HashSet<>()));
        Assert.assertNotNull(list);
        Assert.assertTrue(CollectionUtils.isEmpty(list));
    }

    @Test(groups = "functional", dependsOnMethods = {"testGetMetadataOnlyActionAndJobIds"})
    public void testGetNoCancelActionAndJobIds() {
        when(workflowProxy.getWorkflowExecutionsByJobPids(anyList(), anyString())).thenReturn(generateJobs());
        List<Long> list = toActionPids(
                processAnalyzeWorkflowSubmitter.getCompletedActions(customerSpace, generateCancelActions(), new HashSet<>()));
        Assert.assertNotNull(list);
        log.info(String.format("actionIds=%s", list));

        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0), METADATA_ACTION_PID);
    }

    @Test(groups = "functional", dependsOnMethods = {"testGetMetadataOnlyActionAndJobIds"})
    public void testGetCancelActionAndJobIds() {
        when(workflowProxy.getWorkflowExecutionsByJobPids(anyList(), anyString())).thenReturn(generateJobs());
        List<Long> list = processAnalyzeWorkflowSubmitter.getCanceledActionIds(customerSpace, generateCancelActions());
        Assert.assertNotNull(list);
        log.info(String.format("actionIds=%s", list));

        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
        Assert.assertEquals(list.size(), 2);
    }

    @Test(groups = "functional")
    public void testGetMetadataOnlyActionAndJobIds() {
        when(actionService.findByOwnerId(nullable(Long.class))).thenReturn(generateMetadataChangeActions());
        List<Long> list = toActionPids(
                processAnalyzeWorkflowSubmitter.getCompletedActions(customerSpace, generateMetadataChangeActions(), new HashSet<>()));
        Assert.assertNotNull(list);
        log.info(String.format("actionIds=%s", list));
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0), METADATA_ACTION_PID);
    }

    @Test(groups = "functional", dependsOnMethods = {"testGetMetadataOnlyActionAndJobIds"})
    public void testGetFullActionAndJobIds() {
        when(actionService.findByOwnerId(nullable(Long.class))).thenReturn(generateFullActions());
        when(dataFeedTaskService.getDataFeedTask(anyString(), nullable(String.class))).thenReturn(generateImportDataFeedTask());
        when(workflowProxy.getWorkflowExecutionsByJobPids(anyList(), anyString())).thenReturn(generateJobs());
        List<Long> list = toActionPids(
                processAnalyzeWorkflowSubmitter.getCompletedActions(customerSpace, actionService.findByOwnerId(null), new HashSet<>()));
        Assert.assertNotNull(list);
        log.info(String.format("actionIds=%s", list));

        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
        Assert.assertEquals(list.size(), 3);
        Assert.assertEquals(list.get(0), METADATA_ACTION_PID);
        Assert.assertEquals(list.get(1), COMPLETE_ACTION_1_PID);
        Assert.assertEquals(list.get(2), COMPLETE_ACTION_2_PID);
    }

    @Test(groups = "functional", dependsOnMethods = {"testGetFullActionAndJobIds"})
    public void testGetProblematicActionWithoutTrackingId() {
        List<String> workflowIdStr = Stream.of(RUNNING_ACTION_1_TRACKING_PID, RUNNING_ACTION_2_TRACKING_PID,
                COMPLETE_ACTION_1_TRACKING_PID, COMPLETE_ACTION_2_TRACKING_PID).map(Object::toString)
                .collect(Collectors.toList());
        when(workflowProxy.getWorkflowExecutionsByJobPids(workflowIdStr)).thenReturn(generateJobs());
        when(dataFeedTaskService.getDataFeedTask(anyString(), nullable(String.class))).thenReturn(generateImportDataFeedTask());
        List<Long> list = toActionPids(
                processAnalyzeWorkflowSubmitter.getCompletedActions(customerSpace, generateActionWithTrackingPid(), new HashSet<>()));
        Assert.assertNotNull(list);
        log.info(String.format("actionIds=%s", list));
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
        Assert.assertEquals(list.size(), 3);
        Assert.assertEquals(list.get(0), METADATA_ACTION_PID);
        Assert.assertEquals(list.get(1), COMPLETE_ACTION_1_PID);
        Assert.assertEquals(list.get(2), COMPLETE_ACTION_2_PID);
    }

    @Test(groups = "functional", dependsOnMethods = {"testGetMetadataOnlyActionAndJobIds"})
    public void testNullDataFeedTask() {
        when(actionService.findByOwnerId(nullable(Long.class))).thenReturn(generateFullActions());
        when(dataFeedTaskService.getDataFeedTask(anyString(), eq("NullUniqueId"))).thenReturn(generateNullDataFeedTask());
        when(dataFeedTaskService.getDataFeedTask(anyString(), eq("UniqueId"))).thenReturn(generateImportDataFeedTask());
        when(workflowProxy.getWorkflowExecutionsByJobPids(anyList(), anyString())).thenReturn(generateJobs());
        List<Long> list = toActionPids(
                processAnalyzeWorkflowSubmitter.getCompletedActions(customerSpace, actionService.findByOwnerId(null), new HashSet<>()));
        Assert.assertNotNull(list);
        log.info(String.format("actionIds=%s", list));

        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
        Assert.assertEquals(list.size(), 2);
        Assert.assertEquals(list.get(0), METADATA_ACTION_PID);
        Assert.assertEquals(list.get(1), COMPLETE_ACTION_1_PID);
    }

    @Test(groups = "functional")
    public void testNonWorkFlowDeleteActions() {
        when(actionService.findByOwnerId(nullable(Long.class))).thenReturn(generateNewFullActions());
        when(dataFeedTaskService.getDataFeedTask(anyString(), nullable(String.class))).thenReturn(generateImportDataFeedTask());
        when(workflowProxy.getWorkflowExecutionsByJobPids(anyList(), anyString())).thenReturn(generateJobs());
        Set<BusinessEntity> needDeletedEntities = new HashSet<>();
        List<Long> list = toActionPids(
                processAnalyzeWorkflowSubmitter.getCompletedActions(customerSpace, actionService.findByOwnerId(null),
                        needDeletedEntities));
        Assert.assertNotNull(list);
        Assert.assertNotNull(needDeletedEntities);
        log.info("actionIds={}", list);
        log.info("needDeletedEntities={}", JsonUtils.serialize(needDeletedEntities));

        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
        Assert.assertEquals(list.size(), 4);
        Assert.assertEquals(list.get(0), METADATA_ACTION_PID);
        Assert.assertEquals(list.get(1), COMPLETE_ACTION_1_PID);
        Assert.assertEquals(list.get(2), COMPLETE_ACTION_2_PID);
        Assert.assertEquals(list.get(3), NEW_DELETE_ACTION_PID);
        Assert.assertEquals(needDeletedEntities.size(), 1);
        Assert.assertTrue(needDeletedEntities.contains(BusinessEntity.Account));
    }

    @Test(groups = "functional", dataProvider = "provideInheritableActionTestObjects")
    public void testGetInheritableActionsFromLastFailedPA(DataFeedExecution dataFeedExecution, Job workflowExection,
                                                          List<Action> actions, List<Long> inheritableActionIds) {
        when(dataFeedService.getLatestExecution(anyString(), any())).thenReturn(dataFeedExecution);
        when(workflowProxy.getWorkflowExecution(anyString(), anyBoolean())).thenReturn(workflowExection);
        when(actionService.findByOwnerId(workflowExection.getPid())).thenReturn(actions);

        List<Long> actionIds = processAnalyzeWorkflowSubmitter //
                .getActionsFromLastFailedPA(customerSpace, false, null) //
                .stream() //
                .map(Action::getPid) //
                .collect(Collectors.toList());

        Assert.assertNotNull(actionIds);
        Assert.assertEquals(actionIds, inheritableActionIds);
    }

    @DataProvider(name = "provideInheritableActionTestObjects")
    private Object[][] provideInheritableActionTestObjects() {
        return new Object[][]{
                // completed PA's actions will not be inherited
                {newDataFeedExecution(), newWorkflowExecution(JobStatus.COMPLETED), Collections.emptyList(),
                        Collections.emptyList()},
                {newDataFeedExecution(), newWorkflowExecution(JobStatus.COMPLETED),
                        newTypedActions(ActionType.INTENT_CHANGE, ActionType.ACTIVITY_METRICS_CHANGE),
                        Collections.emptyList()},
                // failed PA
                {newDataFeedExecution(), newWorkflowExecution(JobStatus.FAILED), Collections.emptyList(),
                        Collections.emptyList()},
                {newDataFeedExecution(), newWorkflowExecution(JobStatus.FAILED),
                        newTypedActions(ActionType.ACTIVITY_METRICS_CHANGE, ActionType.ATTRIBUTE_MANAGEMENT_ACTIVATION,
                                ActionType.INTENT_CHANGE, // system action, not
                                // inherited
                                ActionType.ATTRIBUTE_MANAGEMENT_DEACTIVATION, ActionType.METADATA_CHANGE,
                                ActionType.METADATA_SEGMENT_CHANGE, ActionType.DATA_CLOUD_CHANGE, // system
                                // action,
                                // not
                                // inherited
                                ActionType.RATING_ENGINE_CHANGE, ActionType.CDL_DATAFEED_IMPORT_WORKFLOW), // import
                        // action,
                        // not
                        // inherited
                        Arrays.asList(0L, 1L, 3L, 4L, 5L, 7L)},};
    }

    @DataProvider(name = "testTenantLockDataProvider")
    private Object[][] testTenantLockDataProvider() {
        return new Object[][]{{generateLockedTenant()}, {generateUnlockedTenant()}};
    }

    private List<Long> toActionPids(@NotNull List<Action> actions) {
        return actions.stream().map(Action::getPid).collect(Collectors.toList());
    }

    private List<Action> newTypedActions(ActionType... types) {
        return LongStream.range(0, types.length).mapToObj(idx -> {
            Action action = new Action();
            action.setPid(idx);
            action.setType(types[(int) idx]);
            return action;
        }).collect(Collectors.toList());
    }

    private DataFeedExecution newDataFeedExecution() {
        DataFeedExecution execution = new DataFeedExecution();
        execution.setWorkflowId(DEFAULT_WORKFLOW_ID);
        return execution;
    }

    private Job newWorkflowExecution(JobStatus status) {
        Job job = new Job();
        job.setPid(DEFAULT_WORKFLOW_PID);
        job.setId(DEFAULT_WORKFLOW_ID);
        job.setJobStatus(status);
        return job;
    }

    private List<Job> generateJobs() {
        List<Job> jobs = new ArrayList<>();
        Job runningJob1 = new Job();
        runningJob1.setPid(RUNNING_ACTION_1_TRACKING_PID);
        runningJob1.setId(RUNNING_ACTION_1_TRACKING_ID);
        runningJob1.setJobStatus(JobStatus.RUNNING);
        Job runningJob2 = new Job();
        runningJob2.setPid(RUNNING_ACTION_2_TRACKING_PID);
        runningJob2.setId(RUNNING_ACTION_2_TRACKING_ID);
        runningJob2.setJobStatus(JobStatus.PENDING);
        Job completeJob1 = new Job();
        completeJob1.setPid(COMPLETE_ACTION_1_TRACKING_PID);
        completeJob1.setId(COMPLETE_ACTION_1_TRACKING_ID);
        completeJob1.setJobStatus(JobStatus.COMPLETED);
        Job completeJob2 = new Job();
        completeJob2.setPid(COMPLETE_ACTION_2_TRACKING_PID);
        completeJob2.setId(COMPLETE_ACTION_2_TRACKING_ID);
        completeJob2.setJobStatus(JobStatus.FAILED);
        jobs.add(runningJob1);
        jobs.add(runningJob2);
        jobs.add(completeJob1);
        jobs.add(completeJob2);
        return jobs;
    }

    private List<Action> generateFullActions() {
        List<Action> actions = new ArrayList<>(generateMetadataChangeActions());
        ImportActionConfiguration importActionConfiguration1 = new ImportActionConfiguration();
        importActionConfiguration1.setDataFeedTaskId("UniqueId");
        Action runningAction1 = new Action();
        runningAction1.setPid(RUNNING_ACTION_1_PID);
        runningAction1.setTrackingPid(RUNNING_ACTION_1_TRACKING_PID);
        runningAction1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        runningAction1.setActionConfiguration(importActionConfiguration1);
        Action runningAction2 = new Action();
        runningAction2.setPid(RUNNING_ACTION_2_PID);
        runningAction2.setTrackingPid(RUNNING_ACTION_2_TRACKING_PID);
        runningAction2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        runningAction2.setActionConfiguration(importActionConfiguration1);

        Action completeAction1 = new Action();
        completeAction1.setPid(COMPLETE_ACTION_1_PID);
        completeAction1.setTrackingPid(COMPLETE_ACTION_1_TRACKING_PID);
        completeAction1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        completeAction1.setActionConfiguration(importActionConfiguration1);
        Action completeAction2 = new Action();
        completeAction2.setPid(COMPLETE_ACTION_2_PID);
        completeAction2.setTrackingPid(COMPLETE_ACTION_2_TRACKING_PID);
        completeAction2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        ImportActionConfiguration importActionConfiguration2 = new ImportActionConfiguration();
        importActionConfiguration2.setDataFeedTaskId("NullUniqueId");
        completeAction2.setActionConfiguration(importActionConfiguration2);
        actions.add(runningAction1);
        actions.add(runningAction2);
        actions.add(completeAction1);
        actions.add(completeAction2);
        return actions;
    }

    private List<Action> generateMetadataChangeActions() {
        List<Action> actions = new ArrayList<>();
        Action action = new Action();
        action.setPid(METADATA_ACTION_PID);
        action.setType(ActionType.METADATA_CHANGE);
        action.setTrackingPid(null);
        actions.add(action);
        log.info(String.format("generateMetadataChangeActions actions=%s", actions));
        return actions;
    }

    @SuppressWarnings({"unchecked"})
    private List<Action> generateEmptyActions() {
        return Collections.EMPTY_LIST;
    }

    private List<Action> generateActionWithTrackingPid() {
        List<Action> actions = new ArrayList<>(generateFullActions());
        Action action = new Action();
        action.setPid(PROBLEMATIC_ACTION_NO_TRACKING_ID_PID);
        action.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action.setTrackingPid(100L);
        action.setActionConfiguration(new ImportActionConfiguration());
        actions.add(action);
        return actions;
    }

    private List<Action> generateCancelActions() {
        List<Action> actions = new ArrayList<>(generateMetadataChangeActions());
        Action cancelAction1 = new Action();
        cancelAction1.setPid(CANCEL_ACTION_1_PID);
        cancelAction1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        cancelAction1.setActionConfiguration(new ImportActionConfiguration());
        cancelAction1.setActionStatus(ActionStatus.CANCELED);
        Action cancelAction2 = new Action();
        cancelAction2.setPid(CANCEL_ACTION_2_PID);
        cancelAction2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        cancelAction2.setActionConfiguration(new ImportActionConfiguration());
        cancelAction2.setActionStatus(ActionStatus.CANCELED);

        actions.add(cancelAction1);
        actions.add(cancelAction2);
        return actions;
    }

    private List<Action> generateNewFullActions() {
        List<Action> actions = new ArrayList<>(generateMetadataChangeActions());
        Action runningAction1 = new Action();
        runningAction1.setPid(RUNNING_ACTION_1_PID);
        runningAction1.setTrackingPid(RUNNING_ACTION_1_TRACKING_PID);
        runningAction1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        runningAction1.setActionConfiguration(new ImportActionConfiguration());
        Action runningAction2 = new Action();
        runningAction2.setPid(RUNNING_ACTION_2_PID);
        runningAction2.setTrackingPid(RUNNING_ACTION_2_TRACKING_PID);
        runningAction2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        runningAction2.setActionConfiguration(new ImportActionConfiguration());

        Action completeAction1 = new Action();
        completeAction1.setPid(COMPLETE_ACTION_1_PID);
        completeAction1.setTrackingPid(COMPLETE_ACTION_1_TRACKING_PID);
        completeAction1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        completeAction1.setActionConfiguration(new ImportActionConfiguration());
        Action completeAction2 = new Action();
        completeAction2.setPid(COMPLETE_ACTION_2_PID);
        completeAction2.setTrackingPid(COMPLETE_ACTION_2_TRACKING_PID);
        completeAction2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        completeAction2.setActionConfiguration(new ImportActionConfiguration());
        Action completeAction3 = new Action();
        completeAction3.setPid(NEW_DELETE_ACTION_PID);
        completeAction3.setType(ActionType.DATA_REPLACE);
        CleanupActionConfiguration cleanupActionConfiguration = new CleanupActionConfiguration();
        cleanupActionConfiguration.addImpactEntity(BusinessEntity.Account);
        completeAction3.setActionConfiguration(cleanupActionConfiguration);
        actions.add(runningAction1);
        actions.add(runningAction2);
        actions.add(completeAction1);
        actions.add(completeAction2);
        actions.add(completeAction3);
        return actions;
    }

    private Tenant generateLockedTenant() {
        Tenant tenant = new Tenant();
        tenant.setId(lockedCustomerSpace);
        return tenant;
    }

    private Tenant generateUnlockedTenant() {
        Tenant tenant = new Tenant();
        tenant.setId(unlockedCustomerSpace);
        return tenant;
    }

    private DataFeedTask generateImportDataFeedTask() {
        DataFeedTask dataFeedTask = new DataFeedTask();
        dataFeedTask.setEntity(BusinessEntity.Account.name());
        return dataFeedTask;
    }

    private DataFeedTask generateNullDataFeedTask() {
        return null;
    }

}
