package com.latticeengines.apps.cdl.workflow;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class ProcessAnalyzeWorkflowSubmitterTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessAnalyzeWorkflowSubmitterTestNG.class);

    private static final String customerSpace = "tenant";
    private static final Long METADATA_ACTION_PID = 1L;
    private static final Long RUNNING_ACTION_1_PID = 2L;
    private static final Long RUNNING_ACTION_2_PID = 3L;
    private static final Long COMPLETE_ACTION_1_PID = 4L;
    private static final Long COMPLETE_ACTION_2_PID = 5L;
    private static final Long PROBLEMATIC_ACTION_NO_TRACKING_ID_PID = 6L;
    private static final Long RUNNING_ACTION_1_TRACKING_PID = 101L;
    private static final Long RUNNING_ACTION_2_TRACKING_PID = 102L;
    private static final Long COMPLETE_ACTION_1_TRACKING_PID = 103L;
    private static final Long COMPLETE_ACTION_2_TRACKING_PID = 104L;
    private static final Long DEFAULT_WORKFLOW_ID = 200L;
    private static final Long DEFAULT_WORKFLOW_PID = 201L;

    @Mock
    private ActionService actionService;

    @Mock
    private DataFeedProxy dataFeedProxy;

    @Mock
    private WorkflowProxy workflowProxy;

    @InjectMocks
    private ProcessAnalyzeWorkflowSubmitter processAnalyzeWorkflowSubmitter;

    @BeforeTest(groups = "functional")
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test(groups = "functional")
    public void testGetEmptyActionAndJobIds() {
        when(actionService.findByOwnerId(nullable(Long.class))).thenReturn(generateEmptyActions());
        Pair<List<Long>, List<Long>> pair = processAnalyzeWorkflowSubmitter.getActionAndJobIds(customerSpace);
        Assert.assertNotNull(pair);
        Assert.assertTrue(CollectionUtils.isEmpty(pair.getLeft()));
        Assert.assertTrue(CollectionUtils.isEmpty(pair.getRight()));
    }

    @Test(groups = "functional")
    public void testGetMetadataOnlyActionAndJobIds() {
        when(actionService.findByOwnerId(nullable(Long.class))).thenReturn(generateMetadataChangeActions());
        Pair<List<Long>, List<Long>> pair = processAnalyzeWorkflowSubmitter.getActionAndJobIds(customerSpace);
        Assert.assertNotNull(pair);
        log.info(String.format("actionIds=%s", pair.getLeft()));
        log.info(String.format("jobIds=%s", pair.getRight()));
        Assert.assertTrue(CollectionUtils.isNotEmpty(pair.getLeft()));
        Assert.assertEquals(pair.getLeft().size(), 1);
        Assert.assertEquals(pair.getLeft().get(0), METADATA_ACTION_PID);
        Assert.assertTrue(CollectionUtils.isEmpty(pair.getRight()));
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetMetadataOnlyActionAndJobIds" })
    public void testGetFullActionAndJobIds() {
        when(actionService.findByOwnerId(nullable(Long.class))).thenReturn(generateFullActions());
        when(workflowProxy.getWorkflowExecutionsByJobPids(anyList(), anyString())).thenReturn(generateJobs());
        Pair<List<Long>, List<Long>> pair = processAnalyzeWorkflowSubmitter.getActionAndJobIds(customerSpace);
        Assert.assertNotNull(pair);
        log.info(String.format("actionIds=%s", pair.getLeft()));
        log.info(String.format("jobIds=%s", pair.getRight()));
        Assert.assertTrue(CollectionUtils.isNotEmpty(pair.getLeft()));
        Assert.assertEquals(pair.getLeft().size(), 3);
        Assert.assertEquals(pair.getLeft().get(0), METADATA_ACTION_PID);
        Assert.assertEquals(pair.getLeft().get(1), COMPLETE_ACTION_1_PID);
        Assert.assertEquals(pair.getLeft().get(2), COMPLETE_ACTION_2_PID);
        Assert.assertTrue(CollectionUtils.isNotEmpty(pair.getRight()));
        Assert.assertEquals(pair.getRight().size(), 2);
        Assert.assertEquals(pair.getRight().get(0), COMPLETE_ACTION_1_TRACKING_PID);
        Assert.assertEquals(pair.getRight().get(1), COMPLETE_ACTION_2_TRACKING_PID);
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetFullActionAndJobIds" })
    public void testGetProblematicActionWithoutTrackingId() {
        when(actionService.findByOwnerId(nullable(Long.class))).thenReturn(generateActionWithTrackingPid());
        List<String> workflowIdStr = Stream.of(RUNNING_ACTION_1_TRACKING_PID, RUNNING_ACTION_2_TRACKING_PID,
                COMPLETE_ACTION_1_TRACKING_PID, COMPLETE_ACTION_2_TRACKING_PID).map(Object::toString)
                .collect(Collectors.toList());
        when(workflowProxy.getWorkflowExecutionsByJobPids(workflowIdStr)).thenReturn(generateJobs());
        Pair<List<Long>, List<Long>> pair = processAnalyzeWorkflowSubmitter.getActionAndJobIds(customerSpace);
        Assert.assertNotNull(pair);
        log.info(String.format("actionIds=%s", pair.getLeft()));
        log.info(String.format("jobIds=%s", pair.getRight()));
        Assert.assertTrue(CollectionUtils.isNotEmpty(pair.getLeft()));
        Assert.assertEquals(pair.getLeft().size(), 3);
        Assert.assertEquals(pair.getLeft().get(0), METADATA_ACTION_PID);
        Assert.assertEquals(pair.getLeft().get(1), COMPLETE_ACTION_1_PID);
        Assert.assertEquals(pair.getLeft().get(2), COMPLETE_ACTION_2_PID);
        Assert.assertTrue(CollectionUtils.isNotEmpty(pair.getRight()));
        Assert.assertEquals(pair.getRight().size(), 2);
        Assert.assertEquals(pair.getRight().get(0), COMPLETE_ACTION_1_TRACKING_PID);
        Assert.assertEquals(pair.getRight().get(1), COMPLETE_ACTION_2_TRACKING_PID);
    }

    @Test(groups = "functional", dataProvider = "provideInheritableActionTestObjects")
    public void testGetInheritableActionsFromLastFailedPA(DataFeedExecution dataFeedExecution, Job workflowExection,
            List<Action> actions, List<Long> inheritableActionIds) {
        when(dataFeedProxy.getLatestExecution(anyString(), any())).thenReturn(dataFeedExecution);
        when(workflowProxy.getWorkflowExecution(anyString(), anyBoolean())).thenReturn(workflowExection);
        when(actionService.findByOwnerId(workflowExection.getPid())).thenReturn(actions);

        List<Long> actionIds = processAnalyzeWorkflowSubmitter.getActionsFromLastFailedPA(customerSpace).stream()
                .map(Action::getPid).collect(Collectors.toList());

        Assert.assertNotNull(actionIds);
        Assert.assertEquals(actionIds, inheritableActionIds);
    }

    @DataProvider(name = "provideInheritableActionTestObjects")
    private Object[][] provideInheritableActionTestObjects() {
        return new Object[][] {
                // completed PA's actions will not be inherited
                { newDataFeedExecution(), newWorkflowExecution(JobStatus.COMPLETED), Collections.emptyList(),
                        Collections.emptyList() },
                { newDataFeedExecution(), newWorkflowExecution(JobStatus.COMPLETED),
                        newTypedActions(ActionType.INTENT_CHANGE, ActionType.ACTIVITY_METRICS_CHANGE),
                        Collections.emptyList() },
                // failed PA
                { newDataFeedExecution(), newWorkflowExecution(JobStatus.FAILED), Collections.emptyList(),
                        Collections.emptyList() },
                { newDataFeedExecution(), newWorkflowExecution(JobStatus.FAILED),
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
                        Arrays.asList(0L, 1L, 3L, 4L, 5L, 7L) }, };
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
        runningJob1.setId(RUNNING_ACTION_1_TRACKING_PID);
        runningJob1.setJobStatus(JobStatus.RUNNING);
        Job runningJob2 = new Job();
        runningJob2.setId(RUNNING_ACTION_2_TRACKING_PID);
        runningJob2.setJobStatus(JobStatus.PENDING);
        Job completeJob1 = new Job();
        completeJob1.setId(COMPLETE_ACTION_1_TRACKING_PID);
        completeJob1.setJobStatus(JobStatus.COMPLETED);
        Job completeJob2 = new Job();
        completeJob2.setId(COMPLETE_ACTION_2_TRACKING_PID);
        completeJob2.setJobStatus(JobStatus.FAILED);
        jobs.add(runningJob1);
        jobs.add(runningJob2);
        jobs.add(completeJob1);
        jobs.add(completeJob2);
        return jobs;
    }

    private List<Action> generateFullActions() {
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

    @SuppressWarnings({ "unchecked" })
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

}
