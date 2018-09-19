package com.latticeengines.pls.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class WorkflowJobServiceImplUnitTestNG {

    @Mock
    private WorkflowProxy workflowProxy;

    @Mock
    private SourceFileProxy sourceFileProxy;

    @Mock
    private TenantEntityMgr tenantEntityMgr;

    @Mock
    private ActionProxy actionProxy;

    @Mock
    private BatonService batonService;

    @Mock
    private DataFeedProxy dataFeedProxy;

    @InjectMocks
    private WorkflowJobServiceImpl workflowJobService;

    @Mock
    private RatingEngineProxy ratingEngineProxy;

    @Mock
    private ModelSummaryProxy modelSummaryProxy;

    private static final Logger log = LoggerFactory.getLogger(WorkflowJobServiceImplUnitTestNG.class);

    private Long[] jobIds = { 123L, 456L };

    private static final String INITIATOR = "test@lattice-engines.com";

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockWorkflowProxy();
        mockSourceFileEntityManager();
        mockTenantEntityManager();
        mockActionService();
        mockDataFeedProxy();
        mockModelSummaryProxy();

        Tenant tenant = tenantEntityMgr.findByTenantId("tenant");
        MultiTenantContext.setTenant(tenant);
    }

    @Test(groups = "unit")
    public void testFind() {
        Job job = workflowJobService.find("1001", true);
        assertEquals(job.getId(), jobIds[0]);
        assertNotNull(job.getInputs());
        assertEquals(job.getJobStatus(), JobStatus.RUNNING);
        assertEquals(job.getJobType(), "importMatchAndModelWorkflow");
        assertEquals(job.getName(), "importMatchAndModelWorkflow");
        assertEquals(job.getDescription(), "importMatchAndModelWorkflow");
        List<JobStep> steps = job.getSteps();
        assertEquals(steps.size(), 3);
        for (JobStep step : steps) {
            if (step.getJobStepType().equalsIgnoreCase("importdata")) {
                assertEquals(step.getName(), "load_data");
                assertEquals(step.getDescription(), "load_data");
            } else if (step.getJobStepType().equalsIgnoreCase("createeventtablereport")) {
                assertEquals(step.getName(), "load_data");
                assertEquals(step.getDescription(), "load_data");
            } else if (step.getJobStepType().equalsIgnoreCase("createprematcheventtablereport")) {
                assertEquals(step.getName(), "generate_insights");
                assertEquals(step.getDescription(), "generate_insights");
            }
        }
        assertEquals(job.getNumDisplayedSteps().intValue(), 2);
    }

    @Test(groups = "unit")
    public void testFindByJobIds() {
        List<String> jobIdStrs = Arrays.stream(jobIds).map(String::valueOf).collect(Collectors.toList());
        log.info(String.format("jobIdStrs are %s", jobIdStrs));
        List<Job> jobs = workflowJobService.findByJobIds(jobIdStrs);
        assertNotNull(jobs);
        assertEquals(jobs.size(), jobIds.length);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "unit", dependsOnMethods = { "testExpandActions" })
    public void testGenerateUnstartedProcessAnalyzeJob() {
        // Auto Schedule is off
        when(batonService.isEnabled(any(CustomerSpace.class), any(LatticeFeatureFlag.class))).thenReturn(false);
        Job job = workflowJobService.generateUnstartedProcessAnalyzeJob(false);
        testUnstartedPnAJob(job);
        Assert.assertNotNull(job.getInputs());
        Assert.assertNotNull(job.getInputs().get(WorkflowContextConstants.Inputs.ACTION_IDS));
        List<Object> listObj = JsonUtils.deserialize(job.getInputs().get(WorkflowContextConstants.Inputs.ACTION_IDS),
                List.class);
        Assert.assertEquals(listObj.size(), 4);
        Assert.assertNull(job.getNote());
        log.info(String.format("listObj is %s", listObj));

        // Auto Schedule is on
        when(batonService.isEnabled(any(CustomerSpace.class), any(LatticeFeatureFlag.class))).thenReturn(true);
        job = workflowJobService.generateUnstartedProcessAnalyzeJob(true);
        Assert.assertNotNull(job.getSubJobs());
        Assert.assertEquals(job.getSubJobs().size(), 4);
        Assert.assertNotNull(job.getNote());
        log.info("Note is " + job.getNote());

        // test when no action, we still have P&A job
        when(actionProxy.getActionsByOwnerId(anyString(), isNull())).thenReturn(Collections.EMPTY_LIST);
        job = workflowJobService.generateUnstartedProcessAnalyzeJob(false);
        testUnstartedPnAJob(job);
    }

    private void testUnstartedPnAJob(Job job) {
        Date now = new DateTime().toDate();
        Assert.assertNotNull(job);
        Assert.assertEquals(job.getName(), "processAnalyzeWorkflow");
        Assert.assertEquals(job.getJobType(), "processAnalyzeWorkflow");
        Assert.assertEquals(job.getJobStatus(), JobStatus.READY);
        Assert.assertEquals(job.getId(), WorkflowJobServiceImpl.UNSTARTED_PROCESS_ANALYZE_ID);
        Assert.assertTrue(job.getStartTimestamp().after(now));
    }

    @Test(groups = "unit", dependsOnMethods = { "testGenerateUnstartedProcessAnalyzeJob" })
    public void updateStartTimeStampAndForJob() {
        DateTime nextInvokeDate = new DateTime().plusDays(1).withTimeAtStartOfDay();

        Job job = new Job();
        when(batonService.isEnabled(any(CustomerSpace.class), any(LatticeFeatureFlag.class))).thenReturn(true);
        DateTime now = new DateTime();
        log.info("now = " + now.toDate());
        when(dataFeedProxy.nextInvokeTime(anyString())).thenReturn(null);
        workflowJobService.updateStartTimeStampAndForJob(job);
        log.info("job.startDate = " + job.getStartTimestamp());
        Assert.assertTrue(nextInvokeDate.isEqual(job.getStartTimestamp().getTime()));

        long previous25hour = new DateTime().minusHours(25).toDate().getTime();
        when(dataFeedProxy.nextInvokeTime(anyString())).thenReturn(previous25hour);
        log.info("hour of day is " + new DateTime(previous25hour).getHourOfDay());
        workflowJobService.updateStartTimeStampAndForJob(job);
        log.info("job.startDate = " + job.getStartTimestamp());
        Assert.assertTrue(now.isBefore(job.getStartTimestamp().getTime()));
        Assert.assertTrue(new DateTime(previous25hour)
                .getHourOfDay() == (new DateTime(job.getStartTimestamp().getTime()).getHourOfDay()));
        Assert.assertTrue(new DateTime(previous25hour)
                .getMinuteOfHour() == (new DateTime(job.getStartTimestamp().getTime()).getMinuteOfHour()));
        Assert.assertTrue(new DateTime(previous25hour)
                .getSecondOfMinute() == (new DateTime(job.getStartTimestamp().getTime()).getSecondOfMinute()));

        when(dataFeedProxy.nextInvokeTime(anyString())).thenReturn(new DateTime().plusHours(1).toDate().getTime());
        workflowJobService.updateStartTimeStampAndForJob(job);
        now = new DateTime();
        DateTime next2hours = new DateTime().plusHours(2);
        log.info("job.startDate = " + job.getStartTimestamp());
        Assert.assertTrue(now.isBefore(job.getStartTimestamp().getTime())
                && next2hours.isAfter(job.getStartTimestamp().getTime()));

    }

    @Test(groups = "unit")
    public void testGetActionIdsForJob() {
        List<Long> actionIds = workflowJobService.getActionIdsForJob(createProcessAnalyzeJob(jobIds[0]));
        Assert.assertEquals(actionIds.size(), 3);
        Assert.assertTrue(actionIds.contains(101L) && actionIds.contains(102L) && actionIds.contains(103L));
        log.info(String.format("actionIds=%s", actionIds));
    }

    @Test(groups = "unit")
    public void testExpandActions() {
        List<Action> actions = generateActions();
        List<Job> expandedJobs = workflowJobService.expandActions(actions);
        Assert.assertEquals(actions.size(), 5);
        Assert.assertEquals(expandedJobs.size(), 4);
        Job firstJob = expandedJobs.get(0);
        Assert.assertEquals(firstJob.getName(), ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.getDisplayName());
        Assert.assertEquals(firstJob.getJobType(), ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.getName());
        Assert.assertEquals(firstJob.getUser(), INITIATOR);
        Assert.assertEquals(firstJob.getJobStatus(), JobStatus.RUNNING);

        Job secondJob = expandedJobs.get(1);
        Assert.assertEquals(secondJob.getName(), ActionType.METADATA_CHANGE.getDisplayName());
        Assert.assertEquals(secondJob.getJobType(), ActionType.METADATA_CHANGE.getName());
        Assert.assertEquals(secondJob.getUser(), INITIATOR);
        Assert.assertEquals(secondJob.getJobStatus(), JobStatus.COMPLETED);
        log.info(String.format("expandedJobs=%s", expandedJobs));
    }

    @Test(groups = "unit")
    public void testFindJobsBasedOnActionIdsAndType() {
        List<Job> jobs = workflowJobService.findJobsBasedOnActionIdsAndType(Arrays.asList(1L, 2L, 3L, 4L),
                ActionType.CDL_OPERATION_WORKFLOW);
        Assert.assertTrue(CollectionUtils.isEmpty(jobs));
        jobs = workflowJobService.findJobsBasedOnActionIdsAndType(Arrays.asList(1L, 2L, 3L, 4L),
                ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Assert.assertFalse(CollectionUtils.isEmpty(jobs));
        Assert.assertEquals(jobs.size(), 3);
        jobs = workflowJobService.findJobsBasedOnActionIdsAndType(null, null);
        Assert.assertNotNull(jobs);
        Assert.assertEquals(jobs.size(), 0);
    }

    @Test(groups = "unit")
    public void testUpdateJobWithSubJobsIfIsPnA() {
        Job job1 = createProcessAnalyzeJob(jobIds[0]);
        workflowJobService.updateJobWithSubJobsIfIsPnA(job1);
        Assert.assertNotNull(job1.getSubJobs());
        job1 = createProcessAnalyzeJob(jobIds[0]);
        job1.setJobType("bulkmatchworkflow");
        workflowJobService.updateJobWithSubJobsIfIsPnA(job1);
        Assert.assertNull(job1.getSubJobs());
    }

    private void mockWorkflowProxy() {
        when(workflowProxy.getWorkflowExecution(anyString(), anyString())).thenReturn(createJob(jobIds[0]));
        when(workflowProxy.getWorkflowExecution(anyString())).thenReturn(createJob(jobIds[0]));
        List<Job> jobs = new ArrayList<>();
        jobs.add(createJob(jobIds[0]));
        jobs.add(createJob(jobIds[1]));
        when(workflowProxy.getWorkflowExecutionsByJobIds(anyList(), anyString())).thenReturn(jobs);
    }

    private void mockSourceFileEntityManager() {
        when(sourceFileProxy.findByApplicationId(any(), anyString())).thenReturn(new SourceFile());
    }

    private void mockModelSummaryProxy() {
        when(modelSummaryProxy.getByModelId(anyString())).thenReturn(new ModelSummary());
    }

    private void mockTenantEntityManager() {
        Tenant tenant = new Tenant();
        tenant.setId("tenant");
        tenant.setPid(1L);
        when(tenantEntityMgr.findByTenantId(anyString())).thenReturn(tenant);
    }

    private void mockActionService() {
        when(actionProxy.getActionsByOwnerId(anyString(), isNull())).thenReturn(generateActions());
        when(actionProxy.getActionsByPids(anyString(), anyList())).thenReturn(generateActions());
    }

    private void mockDataFeedProxy() {
        when(dataFeedProxy.nextInvokeTime(anyString())).thenReturn(new DateTime().plusDays(1).toDate().getTime());
    }

    private List<Action> generateActions() {
        List<Action> actions = new ArrayList<>();
        Action action1 = new Action();
        action1.setPid(1L);
        action1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action1.setActionConfiguration(new ImportActionConfiguration());
        action1.setActionInitiator(INITIATOR);
        Action action2 = new Action();
        action2.setPid(2L);
        action2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action2.setActionConfiguration(new ImportActionConfiguration());
        action2.setTrackingId(jobIds[0]);
        Action action3 = new Action();
        action3.setPid(3L);
        action3.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action3.setActionConfiguration(new ImportActionConfiguration());
        action3.setTrackingId(jobIds[1]);
        Action action4 = new Action();
        action4.setPid(4L);
        action4.setType(ActionType.METADATA_CHANGE);
        action4.setActionConfiguration(new RatingEngineActionConfiguration());
        action4.setActionInitiator(INITIATOR);
        Action action5 = new Action();
        action5.setPid(5L);
        action5.setType(ActionType.RATING_ENGINE_CHANGE);
        ActionConfiguration actionConfig = new RatingEngineActionConfiguration();
        actionConfig.setHiddenFromUI(true);
        action5.setActionInitiator(INITIATOR);
        action5.setActionConfiguration(actionConfig);
        actions.add(action1);
        actions.add(action2);
        actions.add(action3);
        actions.add(action4);
        actions.add(action5);
        return actions;
    }

    private Job createJob(Long jobId) {
        JobStep stepImportData = new JobStep();
        stepImportData.setJobStepType("importdata");
        stepImportData.setStepStatus(JobStatus.COMPLETED);

        JobStep stepCreateEventTableReport = new JobStep();
        stepCreateEventTableReport.setJobStepType("createeventtablereport");
        stepCreateEventTableReport.setStepStatus(JobStatus.RUNNING);

        JobStep stepCreatePrematchEventTableReport = new JobStep();
        stepCreatePrematchEventTableReport.setJobStepType("createprematcheventtablereport");
        stepCreatePrematchEventTableReport.setStepStatus(JobStatus.PENDING);

        List<JobStep> steps = new ArrayList<>();
        steps.add(stepImportData);
        steps.add(stepCreateEventTableReport);
        steps.add(stepCreatePrematchEventTableReport);

        Job job = new Job();
        job.setId(jobId);
        job.setName("importMatchAndModelWorkflow");
        job.setDescription("importMatchAndModelWorkflow");
        job.setJobType("importMatchAndModelWorkflow");
        job.setStartTimestamp(new Date());
        job.setJobStatus(JobStatus.RUNNING);
        job.setSteps(steps);

        return job;
    }

    private Job createProcessAnalyzeJob(Long jobId) {
        Job job = new Job();
        job.setId(jobId);
        job.setName("processAnalyzeWorkflow");
        job.setDescription("processAnalyzeWorkflow");
        job.setJobType("processAnalyzeWorkflow");
        job.setStartTimestamp(new Date());
        job.setJobStatus(JobStatus.COMPLETED);
        List<Long> actionIds = Arrays.asList(101L, 102L, 103L);
        Map<String, String> inputContext = new HashMap<>();
        inputContext.put(WorkflowContextConstants.Inputs.ACTION_IDS, actionIds.toString());
        job.setInputs(inputContext);
        return job;
    }

    @Test(groups = "unit")
    public void testUpdateJobWithRatingEngine() {
        String ratingEngineId = "engine_hcnrj_a3qfsaty3puoih1q";
        String oldRatingEngineName = "oldName";
        // String newRatingEngineName = "newName";
        Job job = new Job();
        job.setJobType("customEventModelingWorkflow");
        Map<String, String> inputs = new HashMap<>();
        job.setInputs(inputs);
        inputs.put(WorkflowContextConstants.Inputs.RATING_ENGINE_ID, ratingEngineId);
        inputs.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, oldRatingEngineName);
        log.info("job is " + job);
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setId(ratingEngineId);
        ratingEngine.setDisplayName("newName");
        when(ratingEngineProxy.getRatingEngine(anyString(), anyString())).thenReturn(ratingEngine);
        workflowJobService.updateJobWithRatingEngine(job);
        Assert.assertEquals(job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME), "newName");
        log.info("new job is " + job);
    }
}
