package com.latticeengines.workflowapi.functionalframework;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

public class MigrateReportAndOutput extends WorkflowTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MigrateReportAndOutput.class);

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private ReportService reportService;

    @Test(groups = "manual")
    public void migrate() {
        List<WorkflowJob> jobs = workflowJobEntityMgr.findAll();
        for (int i = 0; i < jobs.size(); i++) {
            WorkflowJob job = jobs.get(i);
            Long workflowId = job.getWorkflowId();
            if (workflowId == null || job.getPid() < 10093 || job.getUserId().equals("DEFAULT_USER")
                    || StringUtils.isNotEmpty(job.getOutputContextString())
                    || StringUtils.isNotEmpty(job.getReportContextString())) {
                log.info("No workflowId, so skipping: " + job.getPid());
                continue;
            }
            log.info(String.valueOf(job.getPid()));

            try {
                JobExecution jobExecution = jobExplorer.getJobExecution(workflowId);
                getReports(jobExecution, job);
                getOutputs(jobExecution, job);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            workflowJobEntityMgr.update(job);
        }
    }

    @SuppressWarnings("rawtypes")
    private void getReports(JobExecution jobExecution, WorkflowJob workflowJob) throws JsonProcessingException,
            IOException {
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
            JsonNode json = new ObjectMapper().readTree(String.valueOf(contextObj));
            Iterator<Entry<String, JsonNode>> iterator = json.fields();
            Entry<String, JsonNode> entry = null;
            for (; iterator.hasNext();) {
                entry = iterator.next();
                workflowJob.setReportName(entry.getKey(), entry.getValue().asText());
            }

        }
    }

    private void getOutputs(JobExecution jobExecution, WorkflowJob workflowJob) throws JsonProcessingException,
            IOException {
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
            JsonNode json = new ObjectMapper().readTree(String.valueOf(contextObj));
            Iterator<Entry<String, JsonNode>> iterator = json.fields();
            Entry<String, JsonNode> entry = null;
            for (; iterator.hasNext();) {
                entry = iterator.next();
                workflowJob.setOutputContextValue(entry.getKey(), entry.getValue().asText());
            }

        }

    }
}
