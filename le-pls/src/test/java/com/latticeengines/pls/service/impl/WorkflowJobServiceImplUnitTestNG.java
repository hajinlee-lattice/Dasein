package com.latticeengines.pls.service.impl;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
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

    @InjectMocks
    private WorkflowJobServiceImpl workflowJobService;

    private static final Logger log = LoggerFactory.getLogger(WorkflowJobServiceImplUnitTestNG.class);

    private Long[] jobIds = { 123L, 456L };

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockWorkflowProxy();
        mockSourceFileEntityManager();
        mockModelSummaryService();
        mockTenantEntityManager();
    }

    @Test(groups = "unit")
    public void testFind() {
        Job job = workflowJobService.find("test_workflow");
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
}
