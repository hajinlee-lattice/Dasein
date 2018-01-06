package com.latticeengines.apps.cdl.workflow;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
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
    private static final Long RUNNING_ACTION_1_TRACKING_ID = 101L;
    private static final Long RUNNING_ACTION_2_TRACKING_ID = 102L;
    private static final Long COMPLETE_ACTION_1_TRACKING_ID = 103L;
    private static final Long COMPLETE_ACTION_2_TRACKING_ID = 104L;

    @Mock
    private InternalResourceRestApiProxy internalResourceProxy;

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
        when(internalResourceProxy.getActionsByOwnerId(anyString(), nullable(Long.class)))
                .thenReturn(generateEmptyActions());
        Pair<List<Long>, List<Long>> pair = processAnalyzeWorkflowSubmitter.getActionAndJobIds(customerSpace);
        Assert.assertNotNull(pair);
        Assert.assertTrue(CollectionUtils.isEmpty(pair.getLeft()));
        Assert.assertTrue(CollectionUtils.isEmpty(pair.getRight()));
    }

    @Test(groups = "functional")
    public void testGetMetadataOnlyActionAndJobIds() {
        when(internalResourceProxy.getActionsByOwnerId(anyString(), nullable(Long.class)))
                .thenReturn(generateMetadataChangeActions());
        Pair<List<Long>, List<Long>> pair = processAnalyzeWorkflowSubmitter.getActionAndJobIds(customerSpace);
        Assert.assertNotNull(pair);
        log.info(String.format("actionIds=%s", pair.getLeft()));
        log.info(String.format("jobIds=%s", pair.getRight()));
        Assert.assertTrue(CollectionUtils.isNotEmpty(pair.getLeft()));
        Assert.assertEquals(pair.getLeft().size(), 1);
        Assert.assertEquals(pair.getLeft().get(0), METADATA_ACTION_PID);
        Assert.assertTrue(CollectionUtils.isEmpty(pair.getRight()));
    }

    @Test(groups = "functional")
    public void testGetFullActionAndJobIds() {
        when(internalResourceProxy.getActionsByOwnerId(anyString(), nullable(Long.class)))
                .thenReturn(generateFullActions());
        List<String> workflowIdStr = Arrays.asList(RUNNING_ACTION_1_TRACKING_ID, RUNNING_ACTION_2_TRACKING_ID,
                COMPLETE_ACTION_1_TRACKING_ID, COMPLETE_ACTION_2_TRACKING_ID).stream().map(id -> id.toString())
                .collect(Collectors.toList());
        when(workflowProxy.getWorkflowExecutionsByJobIds(workflowIdStr)).thenReturn(generateJobs());
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
        Assert.assertEquals(pair.getRight().get(0), COMPLETE_ACTION_1_TRACKING_ID);
        Assert.assertEquals(pair.getRight().get(1), COMPLETE_ACTION_2_TRACKING_ID);
    }

    @Test(groups = "functional")
    public void testGetProblematicActionWithoutTrackingId() {
        when(internalResourceProxy.getActionsByOwnerId(anyString(), nullable(Long.class)))
                .thenReturn(generateActionWithoutTrackingId());
        List<String> workflowIdStr = Arrays.asList(RUNNING_ACTION_1_TRACKING_ID, RUNNING_ACTION_2_TRACKING_ID,
                COMPLETE_ACTION_1_TRACKING_ID, COMPLETE_ACTION_2_TRACKING_ID).stream().map(id -> id.toString())
                .collect(Collectors.toList());
        when(workflowProxy.getWorkflowExecutionsByJobIds(workflowIdStr)).thenReturn(generateJobs());
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
        Assert.assertEquals(pair.getRight().get(0), COMPLETE_ACTION_1_TRACKING_ID);
        Assert.assertEquals(pair.getRight().get(1), COMPLETE_ACTION_2_TRACKING_ID);
    }

    private List<Job> generateJobs() {
        List<Job> jobs = new ArrayList<>();
        Job runningJob1 = new Job();
        runningJob1.setId(RUNNING_ACTION_1_TRACKING_ID);
        runningJob1.setJobStatus(JobStatus.RUNNING);
        Job runningJob2 = new Job();
        runningJob2.setId(RUNNING_ACTION_2_TRACKING_ID);
        runningJob2.setJobStatus(JobStatus.PENDING);
        Job completeJob1 = new Job();
        completeJob1.setId(COMPLETE_ACTION_1_TRACKING_ID);
        completeJob1.setJobStatus(JobStatus.COMPLETED);
        Job completeJob2 = new Job();
        completeJob2.setId(COMPLETE_ACTION_2_TRACKING_ID);
        completeJob2.setJobStatus(JobStatus.FAILED);
        jobs.add(runningJob1);
        jobs.add(runningJob2);
        jobs.add(completeJob1);
        jobs.add(completeJob2);
        return jobs;
    }

    private List<Action> generateFullActions() {
        List<Action> actions = new ArrayList<>();
        actions.addAll(generateMetadataChangeActions());
        Action runningAction1 = new Action();
        runningAction1.setPid(RUNNING_ACTION_1_PID);
        runningAction1.setTrackingId(RUNNING_ACTION_1_TRACKING_ID);
        runningAction1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Action runningAction2 = new Action();
        runningAction2.setPid(RUNNING_ACTION_2_PID);
        runningAction2.setTrackingId(RUNNING_ACTION_2_TRACKING_ID);
        runningAction2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);

        Action completeAction1 = new Action();
        completeAction1.setPid(COMPLETE_ACTION_1_PID);
        completeAction1.setTrackingId(COMPLETE_ACTION_1_TRACKING_ID);
        completeAction1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Action completeAction2 = new Action();
        completeAction2.setPid(COMPLETE_ACTION_2_PID);
        completeAction2.setTrackingId(COMPLETE_ACTION_2_TRACKING_ID);
        completeAction2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
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
        action.setTrackingId(null);
        actions.add(action);
        log.info(String.format("generateMetadataChangeActions actions=%s", actions));
        return actions;
    }

    @SuppressWarnings({ "unchecked" })
    private List<Action> generateEmptyActions() {
        return Collections.EMPTY_LIST;
    }

    private List<Action> generateActionWithoutTrackingId() {
        List<Action> actions = new ArrayList<>();
        actions.addAll(generateFullActions());
        Action action = new Action();
        action.setPid(PROBLEMATIC_ACTION_NO_TRACKING_ID_PID);
        action.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action.setTrackingId(null);
        actions.add(action);
        return actions;
    }

}
