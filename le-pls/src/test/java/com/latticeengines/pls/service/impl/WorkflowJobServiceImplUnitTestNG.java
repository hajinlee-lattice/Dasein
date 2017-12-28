package com.latticeengines.pls.service.impl;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
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

import org.junit.Assert;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

public class WorkflowJobServiceImplUnitTestNG {

    @Mock
    private WorkflowProxy workflowProxy;

    @Mock
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Mock
    private ModelSummaryService modelSummaryService;

    @Mock
    private TenantEntityMgr tenantEntityMgr;

    @Mock
    private ActionService actionService;

    @InjectMocks
    private WorkflowJobServiceImpl workflowJobService;

    private static final Logger log = LoggerFactory.getLogger(WorkflowJobServiceImplUnitTestNG.class);

    private Long[] jobIds = { 123L, 456L };

    private static final String INITIATOR = "test@lattice-engines.com";

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockWorkflowProxy();
        mockSourceFileEntityManager();
        mockModelSummaryService();
        mockTenantEntityManager();
        mockActionService();
    }

    @Test(groups = "unit")
    public void testFind() {
        Job job = workflowJobService.find("1001");
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
        List<String> jobIdStrs = Arrays.asList(jobIds).stream().map(jobId -> jobId.toString())
                .collect(Collectors.toList());
        log.info(String.format("jobIdStrs are %s", jobIdStrs));
        List<Job> jobs = workflowJobService.findByJobIds(jobIdStrs);
        assertNotNull(jobs);
        assertEquals(jobs.size(), jobIds.length);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "unit", dependsOnMethods = { "testExpandActions" })
    public void testGenerateUnstartedProcessAnalyzeJob() {
        Job job = workflowJobService.generateUnstartedProcessAnalyzeJob(false);
        Assert.assertNotNull(job);
        Assert.assertEquals(job.getNote(), WorkflowJobServiceImpl.CDLNote);
        Assert.assertEquals(job.getName(), "processAnalyzeWorkflow");
        Assert.assertEquals(job.getJobType(), "processAnalyzeWorkflow");
        Assert.assertEquals(job.getJobStatus(), JobStatus.PENDING);
        Assert.assertEquals(job.getId(), WorkflowJobServiceImpl.UNCOMPLETED_PROCESS_ANALYZE_ID);
        Assert.assertNotNull(job.getInputs());
        Assert.assertNotNull(job.getInputs().get(WorkflowContextConstants.Inputs.ACTION_IDS));
        List<Object> listObj = JsonUtils.deserialize(job.getInputs().get(WorkflowContextConstants.Inputs.ACTION_IDS),
                List.class);
        Assert.assertEquals(listObj.size(), 3);
        log.info(String.format("listObj is %s", listObj));
        job = workflowJobService.generateUnstartedProcessAnalyzeJob(true);
        Assert.assertNotNull(job.getSubJobs());
        Assert.assertEquals(job.getSubJobs().size(), 3);
        when(actionService.findByOwnerId(null, null)).thenReturn(Collections.EMPTY_LIST);
        job = workflowJobService.generateUnstartedProcessAnalyzeJob(false);
        Assert.assertNull(job);
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
        List<Job> expandedJobs = workflowJobService.expandActions(generateActions());
        Assert.assertEquals(expandedJobs.size(), 3);
        Job firstJob = expandedJobs.get(0);
        Assert.assertEquals(firstJob.getName(), ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.getName());
        Assert.assertEquals(firstJob.getJobType(), ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.getName());
        Assert.assertEquals(firstJob.getUser(), INITIATOR);
        Assert.assertEquals(firstJob.getJobStatus(), JobStatus.COMPLETED);
        log.info(String.format("expandedJobs=%s", expandedJobs));
    }

    private void mockWorkflowProxy() {
        when(workflowProxy.getWorkflowExecution(anyString())).thenReturn(createJob(jobIds[0]));

        List<Job> jobs = new ArrayList<>();
        jobs.add(createJob(jobIds[0]));
        jobs.add(createJob(jobIds[1]));
        when(workflowProxy.getWorkflowExecutionsForTenant(anyLong())).thenReturn(jobs);
        when(workflowProxy.getWorkflowExecutionsByJobIds(anyList())).thenReturn(jobs);
    }

    private void mockSourceFileEntityManager() {
        when(sourceFileEntityMgr.findByApplicationId(anyString())).thenReturn(new SourceFile());
    }

    private void mockModelSummaryService() {
        when(modelSummaryService.getModelSummaryByModelId(anyString())).thenReturn(new ModelSummary());
    }

    private void mockTenantEntityManager() {
        Tenant tenant = new Tenant();
        tenant.setId("tenant");
        when(tenantEntityMgr.findByTenantId(anyString())).thenReturn(tenant);
    }

    private void mockActionService() {
        when(actionService.findByOwnerId(null, null)).thenReturn(generateActions());
    }

    private List<Action> generateActions() {
        List<Action> actions = new ArrayList<>();
        Action action1 = new Action();
        action1.setPid(1L);
        action1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action1.setActionInitiator(INITIATOR);
        Action action2 = new Action();
        action2.setPid(2L);
        action2.setTrackingId(jobIds[0]);
        Action action3 = new Action();
        action3.setPid(3L);
        action3.setTrackingId(jobIds[1]);
        actions.add(action1);
        actions.add(action2);
        actions.add(action3);
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
}
