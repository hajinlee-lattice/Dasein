package com.latticeengines.workflowapi.service.impl;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.google.common.annotations.VisibleForTesting;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.workflow.*;
import com.latticeengines.common.exposed.workflow.annotation.WithCustomerSpace;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.service.WorkflowContainerService;
import com.latticeengines.workflowapi.service.WorkflowJobService;

@Component("workflowApiWorkflowJobService")
public class WorkflowJobServiceImpl implements WorkflowJobService {
    private static final Logger log = LoggerFactory.getLogger(WorkflowJobServiceImpl.class);
    private static final String CUSTOMER_SPACE = "CustomerSpace";

    @Value("${hadoop.yarn.timeline-service.webapp.address}")
    private String timelineServiceUrl;

    @Autowired
    private LEJobExecutionRetriever leJobExecutionRetriever;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private WorkflowJobUpdateEntityMgr workflowJobUpdateEntityMgr;

    @Autowired
    private ReportService reportService;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private WorkflowContainerService workflowContainerService;

    private static final long HEARTBEAT_FAILURE_THRESHOLD = TimeUnit.MILLISECONDS.convert(5L, TimeUnit.MINUTES);

    @Override
    @WithCustomerSpace
    public WorkflowExecutionId getWorkflowExecutionIdByApplicationId(String customerSpace, String applicationId) {
        log.info(String.format("customerSpace=%s, applicationId=%s", customerSpace, applicationId));
        return workflowJobEntityMgr.findByApplicationId(applicationId).getAsWorkflowId();
    }

    @Override
    @WithCustomerSpace
    public WorkflowStatus getWorkflowStatus(String customerSpace, Long workflowId) {
        log.info(String.format("customerSpace=%s, workflowId=%s", customerSpace, Long.toString(workflowId)));
        return workflowService.getStatus(new WorkflowExecutionId(workflowId));
    }

    @Override
    @WithCustomerSpace
    public JobStatus getJobStatus(String customerSpace, Long workflowId) {
        log.info(String.format("customerSpace=%s, workflowId=%s", customerSpace, Long.toString(workflowId)));
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(workflowId);
        if (workflowJob == null) {
            return null;
        }
        workflowJob = checkLastUpdateTime(Collections.singletonList(workflowJob)).get(0);
        return JobStatus.fromString(workflowJob.getStatus());
    }

    @Override
    @WithCustomerSpace
    public List<JobStatus> getJobStatus(String customerSpace, List<Long> workflowIds) {
        log.info(String.format("customerSpace=%s, workflowIds=%s", customerSpace, JsonUtils.serialize(workflowIds)));
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                workflowIds, null, null);
        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkLastUpdateTime(workflowJobs);
        return workflowJobs.stream().map(job -> JobStatus.fromString(job.getStatus())).collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public Job getJob(String customerSpace, Long workflowId, Boolean includeDetails) {
        log.info(String.format("customerSpace=%s, workflowId=%s, includeDetails=%s",
                customerSpace, Long.toString(workflowId), Boolean.toString(includeDetails)));
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(workflowId);
        if (workflowJob == null) {
            return null;
        }
        workflowJob = checkLastUpdateTime(Collections.singletonList(workflowJob)).get(0);
        return assembleJob(workflowJob, includeDetails);
    }

    @Override
    @WithCustomerSpace
    public Job getJobByApplicationId(String customerSpace, String applicationId, Boolean includeDetails) {
        log.info(String.format("customerSpace=%s, applicationId=%s, includeDetails=%s", customerSpace, applicationId,
                Boolean.toString(includeDetails)));
        WorkflowJob workflowJob = workflowJobEntityMgr.findByApplicationId(applicationId);
        if (workflowJob == null) {
            return null;
        }
        workflowJob = checkLastUpdateTime(Collections.singletonList(workflowJob)).get(0);
        return assembleJob(workflowJob, includeDetails);
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobs(String customerSpace, Boolean includeDetails) {
        log.info(String.format("customerSpace=%s, includeDetails=%s", customerSpace, Boolean.toString(includeDetails)));
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findAll();
        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkLastUpdateTime(workflowJobs);
        return workflowJobs.stream().map(workflowJob -> assembleJob(workflowJob, includeDetails))
                .collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobs(String customerSpace, List<Long> workflowIds, List<String> types,
                             Boolean includeDetails, Boolean hasParentId, Long parentJobId) {
        log.info(String.format("customerSpace=%s, workflowIds=%s, types=%s, includeDetails=%s, hasParentId=%s, " +
                        "parentJobId=%s", customerSpace, JsonUtils.serialize(workflowIds), JsonUtils.serialize(types),
                JsonUtils.serialize(includeDetails), JsonUtils.serialize(hasParentId), JsonUtils.serialize(parentJobId)));
        Optional<List<Long>> optionalWorkflowIds = Optional.ofNullable(workflowIds);
        Optional<List<String>> optionalTypes = Optional.ofNullable(types);
        List<WorkflowJob> workflowJobs;

        if (hasParentId != null && hasParentId) {
            workflowJobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                    optionalWorkflowIds.orElse(null), optionalTypes.orElse(null), parentJobId);
        } else {
            workflowJobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                    optionalWorkflowIds.orElse(null), optionalTypes.orElse(null), null);
        }

        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkLastUpdateTime(workflowJobs);

        return workflowJobs.stream().map(workflowJob -> assembleJob(workflowJob, includeDetails))
                .collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public List<String> getStepNames(String customerSpace, Long workflowId) {
        log.info(String.format("customerSpace=%s, workflowId=%s", customerSpace, Long.toString(workflowId)));
        JobExecution jobExecution = leJobExecutionRetriever.getJobExecution(workflowId);
        if (jobExecution == null) {
            return null;
        }

        return jobExecution.getStepExecutions().stream().map(StepExecution::getStepName).collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public void updateParentJobId(String customerSpace, List<Long> workflowIds, Long parentJobId) {
        log.info(String.format("customerSpace=%s, workflowIds=%s, parentJobId=%s", customerSpace,
                JsonUtils.serialize(workflowIds), Long.toString(parentJobId)));
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(workflowIds,
                null, null);
        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkLastUpdateTime(workflowJobs);
        workflowJobs.forEach(job -> {
            job.setParentJobId(parentJobId);
            workflowJobEntityMgr.updateParentJobId(job);
        });
    }

    @Override
    @WithCustomerSpace
    public ApplicationId submitWorkFlow(String customerSpace, WorkflowConfiguration workflowConfiguration) {
        log.info(String.format("customerSpace=%s, workflowConfiguration=%s",
                customerSpace, JsonUtils.serialize(workflowConfiguration)));
        return workflowContainerService.submitWorkFlow(workflowConfiguration);
    }

    @Override
    @WithCustomerSpace
    public String submitAwsWorkflow(String customerSpace, WorkflowConfiguration workflowConfiguration) {
        log.info(String.format("customerSpace=%s, workflowConfiguration=%s",
                customerSpace, JsonUtils.serialize(workflowConfiguration)));
        return workflowContainerService.submitAwsWorkFlow(workflowConfiguration);
    }

    @Override
    @WithCustomerSpace
    public void stopWorkflow(String customerSpace, Long workflowId) {
        log.info(String.format("customerSpace=%s, workflowId=%s", customerSpace, Long.toString(workflowId)));
        workflowService.stop(new WorkflowExecutionId(workflowId));
    }

    private List<WorkflowJob> checkLastUpdateTime(List<WorkflowJob> workflowJobs) {
        if (workflowJobs == null) {
            return null;
        }

        for (WorkflowJob workflowJob : workflowJobs) {
            if (workflowJob == null) {
                log.warn("Found null workflowJob. Skip checking lastUpdateTime.");
                continue;
            }

            if (JobStatus.fromString(workflowJob.getStatus()).isTerminated()) {
                continue;
            }

            WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowJob.getPid());
            if (jobUpdate != null &&
                    (System.currentTimeMillis() - jobUpdate.getLastUpdateTime()) > HEARTBEAT_FAILURE_THRESHOLD) {
                workflowJob.setStatus(JobStatus.FAILED.name());
                workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);
            }
        }

        return workflowJobs;
    }

    private Job assembleJob(WorkflowJob workflowJob, Boolean includeDetails) {
        Job job = new Job();
        job.setId(workflowJob.getWorkflowId());
        job.setApplicationId(workflowJob.getApplicationId());
        job.setParentId(workflowJob.getParentJobId());
        job.setInputs(workflowJob.getInputContext());
        job.setOutputs(getOutputs(workflowJob));
        job.setReports(getReports(workflowJob));
        job.setUser(workflowJob.getUserId());
        job.setJobStatus(JobStatus.fromString(workflowJob.getStatus()));

        ErrorDetails errorDetails = workflowJob.getErrorDetails();
        if (errorDetails != null) {
            job.setErrorCode(errorDetails.getErrorCode());
            job.setErrorMsg(errorDetails.getErrorMsg());
        }

        if (job.getOutputs() != null && job.getApplicationId() != null) {
            job.getOutputs().put(WorkflowContextConstants.Outputs.YARN_LOG_LINK_PATH,
                    String.format("%s/app/%s", timelineServiceUrl, job.getApplicationId()));
        }

        JobExecution jobExecution = leJobExecutionRetriever.getJobExecution(workflowJob.getWorkflowId(), includeDetails);

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
            job.setStartTimestamp(workflowStatus.getStartTime());
        } else {
            job.setStartTimestamp(null);
        }

        if (job.getJobStatus().isTerminated()) {
            if (workflowStatus != null) {
                job.setEndTimestamp(workflowStatus.getEndTime());
            } else {
                job.setEndTimestamp(null);
            }
        }

        log.info(String.format("Got job with workflowId=%d, status=%s, tenant=%s, applicationId=%s",
                workflowJob.getWorkflowId(), job.getJobStatus(), workflowJob.getTenant().getName(),
                workflowJob.getApplicationId()));

        return job;
    }

    private WorkflowStatus getStatus(JobExecution jobExecution) {
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

    private List<JobStep> getJobSteps(JobExecution jobExecution) {
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

    private List<Report> getReports(WorkflowJob workflowJob) {
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

    private Map<String, String> getOutputs(WorkflowJob workflowJob) {
        Map<String, String> outputs = new HashMap<>();
        Map<String, String> outputContext = workflowJob.getOutputContext();

        for (String key : outputContext.keySet()) {
            outputs.put(key, outputContext.get(key));
        }
        return outputs;
    }

    @VisibleForTesting
    void setWorkflowService(WorkflowService workflowService) {
        this.workflowService = workflowService;
    }

    @VisibleForTesting
    void setWorkflowContainerService(WorkflowContainerService workflowContainerService) {
        this.workflowContainerService = workflowContainerService;
    }

    @VisibleForTesting
    void setLeJobExecutionRetriever(LEJobExecutionRetriever leJobExecutionRetriever) {
        this.leJobExecutionRetriever = leJobExecutionRetriever;
    }
}
