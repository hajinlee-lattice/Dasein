package com.latticeengines.workflow.exposed.util;

import static com.latticeengines.domain.exposed.workflow.WorkflowConstants.LOG_REDIRECT_LINK;
import static com.latticeengines.domain.exposed.workflow.WorkflowConstants.REDIRECT_RESOURCE;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;

import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;

public class WorkflowJobUtils {
    private static final String CUSTOMER_SPACE = "CustomerSpace";
    private static final Date MIGRATE_THRESHOLD = Date.from(ZonedDateTime.of(
            2019, 2, 1, 0, 0, 0, 0, ZoneId.of("UTC-05:00"))
            .toInstant());

    public static Job assembleJob(ReportService reportService, LEJobExecutionRetriever leJobExecutionRetriever,
                            String lpUrl, WorkflowJob workflowJob, Boolean includeDetails) {
        Job job = new Job();
        job.setPid(workflowJob.getPid());
        job.setId(workflowJob.getWorkflowId());
        job.setApplicationId(workflowJob.getApplicationId());
        job.setParentId(workflowJob.getParentJobId());
        job.setInputs(workflowJob.getInputContext());
        job.setOutputs(getOutputs(workflowJob));
        job.setReports(getReports(reportService, workflowJob));
        job.setUser(workflowJob.getUserId());
        if (workflowJob.getStatus() != null) {
            job.setJobStatus(JobStatus.fromString(workflowJob.getStatus()));
        }
        job.setName(workflowJob.getType());

        ErrorDetails errorDetails = workflowJob.getErrorDetails();
        if (errorDetails != null) {
            job.setErrorCode(errorDetails.getErrorCode());
            job.setErrorMsg(errorDetails.getErrorMsg());
        }

        if (job.getOutputs() != null && job.getApplicationId() != null) {
            job.getOutputs().put(WorkflowContextConstants.Outputs.YARN_LOG_LINK_PATH, //
                    logRedirectLink(lpUrl, job.getPid()));
        }

        JobExecution jobExecution = leJobExecutionRetriever.getJobExecution(workflowJob.getWorkflowId(),
                includeDetails);

        if (StringUtils.isNotEmpty(workflowJob.getType())) {
            job.setJobType(workflowJob.getType());
        } else if (jobExecution != null) {
            job.setJobType(jobExecution.getJobInstance().getJobName());
        } else {
            job.setJobType(null);
        }

        // currently only job steps are considered as job details
        if (includeDetails) {
            job.setSteps(getJobSteps(jobExecution));
        } else {
            job.setSteps(null);
        }

        WorkflowStatus workflowStatus = getStatus(jobExecution);
        if (workflowJob.getStartTimeInMillis() != null) {
            job.setStartTimestamp(new Date(workflowJob.getStartTimeInMillis()));
        } else if (workflowStatus != null) {
            Date startTime = workflowStatus.getStartTime();
            if (startTime.compareTo(MIGRATE_THRESHOLD) < 0) {
                startTime = adjustDate(startTime, "UTC-05:00", "UTC");
            }
            job.setStartTimestamp(startTime);
        } else {
            job.setStartTimestamp(null);
        }

        if (job.getJobStatus() != null && job.getJobStatus().isTerminated()) {
            if (workflowStatus != null) {
                Date endTime = workflowStatus.getEndTime();
                if (endTime != null && endTime.compareTo(MIGRATE_THRESHOLD) < 0) {
                    endTime = adjustDate(endTime, "UTC-05:00", "UTC");
                }
                job.setEndTimestamp(endTime);
            } else {
                job.setEndTimestamp(null);
            }
        }

        return job;
    }

    @VisibleForTesting
    public static Date adjustDate(Date dateToAdjust, String fromZone, String toZone) {
        if (dateToAdjust != null) {
            ZonedDateTime fromDatetime = ZonedDateTime.ofInstant(dateToAdjust.toInstant(), ZoneId.of(fromZone));
            ZonedDateTime toDateTime = fromDatetime.toInstant().atZone(ZoneId.of(toZone));
            return Date.from(toDateTime.toInstant());
        } else {
            return null;
        }
    }

    private static String logRedirectLink(String lpUrl, long workflowPid) {
        return lpUrl + "/lp/" + REDIRECT_RESOURCE + LOG_REDIRECT_LINK + String.valueOf(workflowPid);
    }

    public static Job removeJobDetails(Job job) {
        if (job != null) {
            job.setSteps(null);
        }
        return job;
    }

    private static WorkflowStatus getStatus(JobExecution jobExecution) {
        if (jobExecution == null) {
            return null;
        }

        WorkflowStatus workflowStatus = new WorkflowStatus();
        workflowStatus.setStatus(jobExecution.getStatus());
        workflowStatus.setStartTime(jobExecution.getStartTime());
        workflowStatus.setEndTime(jobExecution.getEndTime());
        workflowStatus.setLastUpdated(jobExecution.getLastUpdated());
        workflowStatus.setWorkflowName(jobExecution.getJobInstance().getJobName());

        String customerSpace = jobExecution.getJobParameters().getString(CUSTOMER_SPACE);
        if (StringUtils.isNotEmpty(customerSpace)) {
            workflowStatus.setCustomerSpace(CustomerSpace.parse(customerSpace));
        }

        return workflowStatus;
    }

    private static List<JobStep> getJobSteps(JobExecution jobExecution) {
        if (jobExecution == null) {
            return null;
        }

        List<JobStep> steps = new ArrayList<>();

        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            JobStep jobStep = new JobStep();
            jobStep.setJobStepType(stepExecution.getStepName());
            jobStep.setStepStatus(JobStatus.fromString(stepExecution.getStatus().name()));
            if (stepExecution.getExitStatus() == ExitStatus.NOOP) {
                jobStep.setStepStatus(JobStatus.SKIPPED);
            }
            jobStep.setStartTimestamp(stepExecution.getStartTime());
            jobStep.setEndTimestamp(stepExecution.getEndTime());
            steps.add(jobStep);
        }

        return steps;
    }

    private static List<Report> getReports(ReportService reportService, WorkflowJob workflowJob) {
        List<Report> reports = new ArrayList<>();
        Map<String, String> reportContext = workflowJob.getReportContext();
        for (String reportPurpose : reportContext.keySet()) {
            Report report = reportService.getReportByName(reportContext.get(reportPurpose));
            if (report != null) {
                reports.add(report);
            }
        }
        return reports;
    }

    private static Map<String, String> getOutputs(WorkflowJob workflowJob) {
        Map<String, String> outputs = new HashMap<>();
        Map<String, String> outputContext = workflowJob.getOutputContext();

        for (String key : outputContext.keySet()) {
            outputs.put(key, outputContext.get(key));
        }
        return outputs;
    }

    public static List<String> getWorkflowJobMappingsForJobStatuses(List<String> jobStatuses) {
        if (CollectionUtils.isEmpty(jobStatuses)) {
            return Collections.emptyList();
        }

        Set<String> workflowJobStatuses = new HashSet<>();
        jobStatuses.forEach(jobStatus -> {
            workflowJobStatuses.addAll(JobStatus.mappedWorkflowJobStatuses(jobStatus));
        });
        return new ArrayList<>(workflowJobStatuses);
    }
}
