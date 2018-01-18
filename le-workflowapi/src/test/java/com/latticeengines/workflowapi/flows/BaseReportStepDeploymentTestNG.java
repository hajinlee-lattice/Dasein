package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.workflow.exposed.service.ReportService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.flows.testflows.testreport.TestReportWorkflowConfiguration;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiDeploymentTestNGBase;
import com.latticeengines.workflowapi.service.WorkflowJobService;

public class BaseReportStepDeploymentTestNG extends WorkflowApiDeploymentTestNGBase {

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private WorkflowJobService workflowJobService;

    @Autowired
    private ReportService reportService;

    @Test(groups = "workflow")
    public void testRegisterReport() throws Exception {
        TestReportWorkflowConfiguration configuration = generateConfiguration();
        WorkflowExecutionId workflowId = workflowService.start(configuration);
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
        
        Job job = workflowJobService.getJob(CustomerSpace.parse(mainTestTenant.getId()).toString(), workflowId.getId(), true);
        assertEquals(job.getReports().size(), 1);
        Report report = job.getReports().get(0);
        assertTrue(report.getName().startsWith("Test"));

        Report retrieved = reportService.getReportByName(report.getName());
        assertEquals(report.getJson().toString(), retrieved.getJson().toString());

        Map<String, String> output = job.getOutputs();
        assertEquals(output.size(), 1);
    }

    private TestReportWorkflowConfiguration generateConfiguration() {
        TestReportWorkflowConfiguration.Builder builder = new TestReportWorkflowConfiguration.Builder();
        return builder //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microServiceHostPort) //
                .reportName("Test") //
                .customer(mainTestCustomerSpace) //
                .build();
    }
}
