package com.latticeengines.workflowapi.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import com.latticeengines.common.exposed.workflow.annotation.WithCustomerSpace;
import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
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

    private static final long HEARTBEAT_FAILURE_THRESHOLD =
            TimeUnit.MILLISECONDS.convert(5L, TimeUnit.MINUTES);

    private static final long SPRING_BATCH_FAILURE_THRESHOLD =
            TimeUnit.MILLISECONDS.convert(24L, TimeUnit.HOURS);

    @Override
    @WithCustomerSpace
    public WorkflowExecutionId getWorkflowExecutionIdByApplicationId(String customerSpace, String applicationId) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByApplicationId(applicationId);
        if (workflowJob == null) {
            return null;
        }
        return workflowJob.getAsWorkflowId();
    }

    @Override
    @WithCustomerSpace
    public JobStatus getJobStatusByWorkflowId(String customerSpace, Long workflowId) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(workflowId);
        if (workflowJob == null) {
            return null;
        }
        workflowJob = checkLastUpdateTime(Collections.singletonList(workflowJob)).get(0);
        return JobStatus.fromString(workflowJob.getStatus());
    }

    @Override
    @WithCustomerSpace
    public JobStatus getJobStatusByWorkflowPid(String customerSpace, Long workflowPid) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowPid(workflowPid);
        if (workflowJob == null) {
            return null;
        }
        workflowJob = checkExecutionId(Collections.singletonList(workflowJob)).get(0);
        workflowJob = checkLastUpdateTime(Collections.singletonList(workflowJob)).get(0);
        return JobStatus.fromString(workflowJob.getStatus());
    }

    @Override
    @WithCustomerSpace
    public List<JobStatus> getJobStatusByWorkflowIds(String customerSpace, List<Long> workflowIds) {
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(workflowIds, null,
                null);
        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkLastUpdateTime(workflowJobs);
        return workflowJobs.stream().map(job -> JobStatus.fromString(job.getStatus())).collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public List<JobStatus> getJobStatusByWorkflowPids(String customerSpace, List<Long> workflowPids) {
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowPidsOrTypesOrParentJobId(workflowPids,
                null,null);
        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkExecutionId(workflowJobs);
        workflowJobs = checkLastUpdateTime(workflowJobs);
        return workflowJobs.stream().map(job -> JobStatus.fromString(job.getStatus())).collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public Job getJobByWorkflowId(String customerSpace, Long workflowId, Boolean includeDetails) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(workflowId);
        if (workflowJob == null) {
            return null;
        }
        workflowJob = checkLastUpdateTime(Collections.singletonList(workflowJob)).get(0);
        return assembleJob(workflowJob, includeDetails);
    }

    @Override
    @WithCustomerSpace
    public Job getJobByWorkflowPid(String customerSpace, Long workflowPid, Boolean includeDetails) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowPid(workflowPid);
        if (workflowJob == null) {
            return null;
        }
        workflowJob = checkExecutionId(Collections.singletonList(workflowJob)).get(0);
        workflowJob = checkLastUpdateTime(Collections.singletonList(workflowJob)).get(0);
        return assembleJob(workflowJob, includeDetails);
    }

    @Override
    @WithCustomerSpace
    public Job getJobByApplicationId(String customerSpace, String applicationId, Boolean includeDetails) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByApplicationId(applicationId);
        if (workflowJob == null) {
            return null;
        }
        workflowJob = checkExecutionId(Collections.singletonList(workflowJob)).get(0);
        workflowJob = checkLastUpdateTime(Collections.singletonList(workflowJob)).get(0);
        return assembleJob(workflowJob, includeDetails);
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobsByCustomerSpace(String customerSpace, Boolean includeDetails) {
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findAll();
        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkExecutionId(workflowJobs);
        workflowJobs = checkLastUpdateTime(workflowJobs);
        return workflowJobs.stream().map(workflowJob -> assembleJob(workflowJob, includeDetails))
                .collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobsByWorkflowIds(String customerSpace, List<Long> workflowIds, List<String> types,
                                          Boolean includeDetails, Boolean hasParentId, Long parentJobId) {
        Optional<List<Long>> optionalWorkflowIds = Optional.ofNullable(workflowIds);
        Optional<List<String>> optionalTypes = Optional.ofNullable(types);
        List<WorkflowJob> workflowJobs;

        if (hasParentId != null && hasParentId) {
            workflowJobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(optionalWorkflowIds.orElse(null),
                    optionalTypes.orElse(null), parentJobId);
        } else {
            workflowJobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(optionalWorkflowIds.orElse(null),
                    optionalTypes.orElse(null), null);
        }

        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkLastUpdateTime(workflowJobs);

        return workflowJobs.stream().map(workflowJob -> assembleJob(workflowJob, includeDetails))
                .collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobsByWorkflowPids(String customerSpace, List<Long> workflowPids, List<String> types,
                                           Boolean includeDetails, Boolean hasParentId, Long parentJobId) {
        Optional<List<Long>> optionalWorkflowPids = Optional.ofNullable(workflowPids);
        Optional<List<String>> optionalTypes = Optional.ofNullable(types);
        List<WorkflowJob> workflowJobs;

        if (hasParentId != null && hasParentId) {
            workflowJobs = workflowJobEntityMgr.findByWorkflowPidsOrTypesOrParentJobId(optionalWorkflowPids.orElse(null),
                    optionalTypes.orElse(null), parentJobId);
        } else {
            workflowJobs = workflowJobEntityMgr.findByWorkflowPidsOrTypesOrParentJobId(optionalWorkflowPids.orElse(null),
                    optionalTypes.orElse(null), null);
        }

        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkExecutionId(workflowJobs);
        workflowJobs = checkLastUpdateTime(workflowJobs);

        return workflowJobs.stream().map(workflowJob -> assembleJob(workflowJob, includeDetails))
                .collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public List<String> getStepNames(String customerSpace, Long workflowPid) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowPid(workflowPid);
        Long workflowId = workflowJob != null ? workflowJob.getWorkflowId() : -1L;
        JobExecution jobExecution = leJobExecutionRetriever.getJobExecution(workflowId);
        if (jobExecution == null) {
            return null;
        }

        return jobExecution.getStepExecutions().stream().map(StepExecution::getStepName).collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public void updateParentJobIdByWorkflowIds(String customerSpace, List<Long> workflowIds, Long parentJobId) {
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(workflowIds, null,
                null);
        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkLastUpdateTime(workflowJobs);
        workflowJobs.forEach(job -> {
            job.setParentJobId(parentJobId);
            workflowJobEntityMgr.updateParentJobId(job);
        });
    }

    @Override
    @WithCustomerSpace
    public void updateParentJobIdByWorkflowPids(String customerSpace, List<Long> workflowPids, Long parentJobId) {
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowPidsOrTypesOrParentJobId(workflowPids,
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
    public ApplicationId submitWorkflow(String customerSpace, WorkflowConfiguration workflowConfiguration,
                                        Long workflowPid) {
        return workflowContainerService.submitWorkflow(workflowConfiguration, workflowPid);
    }

    @Override
    @WithCustomerSpace
    public String submitAwsWorkflow(String customerSpace, WorkflowConfiguration workflowConfiguration) {
        return workflowContainerService.submitAwsWorkflow(workflowConfiguration, null);
    }

    @Override
    @WithCustomerSpace
    public Long createWorkflowJob(String customerSpace) {
        log.info("Creating workflowJob with customerSpace=" + customerSpace);
        Long currentTime = System.currentTimeMillis();
        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setTenant(MultiTenantContext.getTenant());
        workflowJob.setStatus(JobStatus.PENDING.name());
        workflowJob.setStartTimeInMillis(currentTime);
        workflowJobEntityMgr.create(workflowJob);

        Long workflowPid = workflowJob.getPid();
        WorkflowJobUpdate jobUpdate = new WorkflowJobUpdate();
        jobUpdate.setWorkflowPid(workflowPid);
        jobUpdate.setCreateTime(currentTime);
        jobUpdate.setLastUpdateTime(currentTime);
        workflowJobUpdateEntityMgr.create(jobUpdate);

        return workflowJob.getPid();
    }

    @Override
    @WithCustomerSpace
    public void stopWorkflow(String customerSpace, Long workflowId) {
        workflowService.stop(new WorkflowExecutionId(workflowId));
    }

    @Override
    @WithCustomerSpace
    public void stopWorkflowJob(String customerSpace, Long workflowPid) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowPid(workflowPid);
        Long workflowId = workflowJob != null ? workflowJob.getWorkflowId() : -1L;
        workflowService.stop(new WorkflowExecutionId(workflowId));
    }

    private List<WorkflowJob> checkExecutionId(List<WorkflowJob> workflowJobs) {
        if (workflowJobs == null) {
            return null;
        }

        for (WorkflowJob workflowJob : workflowJobs) {
            if (workflowJob == null) {
                log.warn("Found null workflowJob. Skip checking executionId.");
                continue;
            }

            if (JobStatus.fromString(workflowJob.getStatus()).isTerminated()) {
                continue;
            }

            if ((System.currentTimeMillis() - workflowJob.getStartTimeInMillis()) > SPRING_BATCH_FAILURE_THRESHOLD
                    && workflowJob.getWorkflowId() == null) {
                workflowJob.setStatus(JobStatus.FAILED.name());
                workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);
            }
        }

        return workflowJobs;
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
            if (jobUpdate != null
                    && (jobUpdate.getLastUpdateTime() - jobUpdate.getCreateTime() >= 1000 * 120)
                    && (System.currentTimeMillis() - jobUpdate.getLastUpdateTime()) > HEARTBEAT_FAILURE_THRESHOLD) {
                workflowJob.setStatus(JobStatus.FAILED.name());
                workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);
            }
        }

        return workflowJobs;
    }

    private Job assembleJob(WorkflowJob workflowJob, Boolean includeDetails) {
        Job job = new Job();
        job.setPid(workflowJob.getPid());
        job.setId(workflowJob.getWorkflowId());
        job.setApplicationId(workflowJob.getApplicationId());
        job.setParentId(workflowJob.getParentJobId());
        job.setInputs(workflowJob.getInputContext());
        job.setOutputs(getOutputs(workflowJob));
        job.setReports(getReports(workflowJob));
        job.setUser(workflowJob.getUserId());
        job.setJobStatus(JobStatus.fromString(workflowJob.getStatus()));
        job.setName(workflowJob.getType());

        ErrorDetails errorDetails = workflowJob.getErrorDetails();
        if (errorDetails != null) {
            job.setErrorCode(errorDetails.getErrorCode());
            job.setErrorMsg(errorDetails.getErrorMsg());
        }

        if (job.getOutputs() != null && job.getApplicationId() != null) {
            job.getOutputs().put(WorkflowContextConstants.Outputs.YARN_LOG_LINK_PATH,
                    String.format("%s/app/%s", timelineServiceUrl, job.getApplicationId()));
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
