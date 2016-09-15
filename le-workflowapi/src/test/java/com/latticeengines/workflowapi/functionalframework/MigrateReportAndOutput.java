package com.latticeengines.workflowapi.functionalframework;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.ReportService;
import com.latticeengines.workflow.functionalframework.WorkflowFunctionalTestNGBase;

public class MigrateReportAndOutput extends WorkflowFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(MigrateReportAndOutput.class);

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private ReportService reportService;

    @Test(groups = "functional")
    public void migrate() {
        List<WorkflowJob> jobs = workflowJobEntityMgr.findAll();
        for (int i = 0; i < jobs.size(); i++) {
            WorkflowJob job = jobs.get(i);
            Long workflowId = job.getWorkflowId();
            if (workflowId == null || job.getUserId().equals("DEFAULT_USER")) {
                log.info("No workflowId, so skipping: " + job.getPid());
                continue;
            }
            log.info(job.getPid());
            JobExecution jobExecution = jobExplorer.getJobExecution(workflowId);
            getReports(jobExecution, job);
            getOutputs(jobExecution, job);
            workflowJobEntityMgr.update(job);
        }
    }

    @SuppressWarnings("rawtypes")
    private void getReports(JobExecution jobExecution, WorkflowJob workflowJob) {
        ExecutionContext context = jobExecution.getExecutionContext();
        Object contextObj = context.get(WorkflowContextConstants.REPORTS);

        if (contextObj == null) {
            return;
        }
        if (contextObj instanceof Map) {
            for (Object obj : ((Map) contextObj).values()) {
                if (obj instanceof String) {
                    Report report = reportService.getReportByName((String) obj);
                    if (report != null) {
                        workflowJob.setReportName(report.getPurpose().getKey(), report.getName());
                    }
                } else {
                    throw new RuntimeException("Failed to convert context object.");
                }
            }
        } else if (contextObj instanceof Set) {
            for (Object obj : (Set) contextObj) {
                if (obj instanceof String) {
                    Report report = reportService.getReportByName((String) obj);
                    if (report != null) {
                        workflowJob.setReportName(report.getPurpose().getKey(), report.getName());
                    }
                } else {
                    throw new RuntimeException("Failed to convert context object.");
                }
            }
        } else {
            throw new RuntimeException("Failed to convert context object.");
        }
    }

    private void getOutputs(JobExecution jobExecution, WorkflowJob workflowJob) {
        ExecutionContext context = jobExecution.getExecutionContext();
        Object contextObj = context.get(WorkflowContextConstants.OUTPUTS);

        if (contextObj == null) {
            return;
        }
        if (contextObj instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) contextObj).entrySet()) {
                if (entry.getKey() instanceof String && entry.getValue() instanceof String) {
                    workflowJob.setOutputContextValue((String) entry.getKey(), (String) entry.getValue());
                } else {
                    throw new RuntimeException("Failed to convert context object to Map<String, String>.");
                }
            }
        } else {
            throw new RuntimeException("Failed to convert context object to Map<String, String>.");
        }

    }
}
