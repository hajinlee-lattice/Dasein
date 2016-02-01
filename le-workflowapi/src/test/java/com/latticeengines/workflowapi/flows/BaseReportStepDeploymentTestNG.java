package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;

import java.util.Map;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.workflow.exposed.service.ReportService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.flows.testflows.testreport.TestRegisterReport;
import com.latticeengines.workflowapi.flows.testflows.testreport.TestReportWorkflow;
import com.latticeengines.workflowapi.flows.testflows.testreport.TestReportWorkflowConfiguration;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class BaseReportStepDeploymentTestNG extends WorkflowApiFunctionalTestNGBase {
    @Autowired
    private TestReportWorkflow testReportWorkflow;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private ReportService reportService;

    @Test(groups = "deployment")
    public void testRegisterReport() throws Exception {
        TestReportWorkflowConfiguration configuration = generateConfiguration();
        WorkflowExecutionId workflowId = workflowService.start(testReportWorkflow.name(), configuration);
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);

        Job job = workflowService.getJob(workflowId);
        assertEquals(job.getReports().size(), 1);
        Report report = job.getReports().get(0);
        assertEquals(report.getName(), new TestRegisterReport().getName());

        Report retrieved = reportService.getReportByName(report.getName());
        assertEquals(report.getJson().toString(), retrieved.getJson().toString());

        Map<String, String> output = job.getOutputs();
        assertEquals(output.size(), 1);
    }

    private TestReportWorkflowConfiguration generateConfiguration() {
        TestReportWorkflowConfiguration.Builder builder = new TestReportWorkflowConfiguration.Builder();
        return builder //
                .microServiceHostPort(microServiceHostPort) //
                .customer(WFAPITEST_CUSTOMERSPACE) //
                .build();
    }
}
