package com.latticeengines.workflowapi.service.impl;

import java.util.*;
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

import com.latticeengines.domain.exposed.workflow.*;
import com.latticeengines.common.exposed.workflow.annotation.WithCustomerSpace;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.ReportService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.exposed.service.WorkflowTenantService;
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
    private ReportService reportService;

    @Autowired
    private WorkflowTenantService workflowTenantService;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private WorkflowContainerService workflowContainerService;

    @Override
    public WorkflowExecutionId getWorkflowExecutionIdByApplicationId(String applicationId) {
        return workflowJobEntityMgr.findByApplicationId(applicationId).getAsWorkflowId();
    }

    @Override
    public WorkflowStatus getWorkflowStatus(Long workflowId) {
        return workflowService.getStatus(new WorkflowExecutionId(workflowId));
    }

    @Override
    public JobStatus getJobStatus(Long workflowId) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(workflowId);
        return (workflowJob != null) ? JobStatus.fromString(workflowJob.getStatus()) : null;
    }

    @Override
    public List<JobStatus> getJobStatus(List<Long> workflowIds) {
        return workflowIds.stream().map(this::getJobStatus).collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public List<JobStatus> getJobStatus(String customerSpace, List<Long> workflowIds) {
        List<WorkflowJob> jobs = workflowJobEntityMgr.findByWorkflowIdsWithFilter(workflowIds);
        jobs.removeIf(Objects::isNull);
        return jobs.stream().map(job -> JobStatus.fromString(job.getStatus())).collect(Collectors.toList());
    }

    @Override
    public Job getJob(Long workflowId) {
        return getJob(workflowId, true);
    }

    @Override
    public Job getJob(Long workflowId, Boolean includeDetails) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(workflowId);
        return assembleJob(workflowJob, includeDetails);
    }

    @Override
    public Job getJobByApplicationId(String applicationId) {
        return getJobByApplicationId(applicationId, true);
    }

    @Override
    public Job getJobByApplicationId(String applicationId, Boolean includeDetails) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByApplicationId(applicationId);
        return assembleJob(workflowJob, includeDetails);
    }

    @Override
    public List<Job> getJobs(List<Long> workflowIds) {
        return getJobs(workflowIds, true);
    }

    @Override
    public List<Job> getJobs(List<Long> workflowIds, Boolean includeDetails) {
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowIds(workflowIds);

        return workflowJobs.stream().map(workflowJob -> assembleJob(workflowJob, includeDetails))
                .collect(Collectors.toList());
    }

    @Override
    public List<Job> getJobs(List<Long> workflowIds, String type) {
        return getJobs(workflowIds, Collections.singletonList(type), true);
    }

    @Override
    public List<Job> getJobs(List<Long> workflowIds, List<String> types, Boolean includeDetails) {
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowIds(workflowIds, types);

        return workflowJobs.stream().map(workflowJob -> assembleJob(workflowJob, includeDetails))
                .collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobs(String customerSpace, List<Long> workflowIds, List<String> types,
                             Boolean includeDetails, Boolean hasParentId, Long parentJobId) {
        List<WorkflowJob> workflowJobs;
        if (hasParentId) {
            workflowJobs = workflowJobEntityMgr.findByWorkflowIdsWithFilter(workflowIds, types, parentJobId);
            workflowJobs.forEach(workflowJob -> workflowJob.setParentJobId(parentJobId));
        } else {
            workflowJobs = workflowJobEntityMgr.findByWorkflowIdsWithFilter(workflowIds, types);
        }

        return workflowJobs.stream().map(workflowJob -> assembleJob(workflowJob, includeDetails))
                .collect(Collectors.toList());
    }

    @Override
    public List<Job> getJobsByTenantPid(Long tenantPid) {
        return getJobsByTenantPid(tenantPid, true);
    }

    @Override
    public List<Job> getJobsByTenantPid(Long tenantPid, Boolean includeDetails) {
        Tenant tenant = workflowTenantService.getTenantByTenantPid(tenantPid);
        MultiTenantContext.setTenant(tenant);
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByTenant(tenant);

        return workflowJobs.stream().map(workflowJob -> assembleJob(workflowJob, includeDetails))
                .collect(Collectors.toList());
    }

    @Override
    public List<Job> getJobsByTenantPid(Long tenantPid, List<String> types, Boolean includeDetails) {
        Tenant tenant = workflowTenantService.getTenantByTenantPid(tenantPid);
        MultiTenantContext.setTenant(tenant);
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByTenant(tenant, types);

        return workflowJobs.stream().map(workflowJob -> assembleJob(workflowJob, includeDetails))
                .collect(Collectors.toList());
    }

    @Override
    public JobStatus getJobStatusByApplicationId(String applicationId) {
        return JobStatus.fromString(workflowJobEntityMgr.findByApplicationId(applicationId).getStatus());
    }

    @Override
    public List<String> getStepNames(Long workflowId) {
        JobExecution jobExecution = leJobExecutionRetriever.getJobExecution(workflowId);
        if (jobExecution == null) {
            return null;
        }

        return jobExecution.getStepExecutions().stream().map(StepExecution::getStepName).collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public List<Job> updateParentJobId(String customerSpace, List<Long> workflowIds, Long parentJobId) {
        List<WorkflowJob> jobs = workflowJobEntityMgr.findByWorkflowIdsWithFilter(workflowIds);
        jobs.removeIf(Objects::isNull);
        jobs.forEach(job -> {
            job.setParentJobId(parentJobId);
            workflowJobEntityMgr.updateParentJobId(job);
        });

        return getJobs(workflowIds);
    }

    @Override
    public ApplicationId submitWorkFlow(WorkflowConfiguration workflowConfiguration) {
        return workflowContainerService.submitWorkFlow(workflowConfiguration);
    }

    @Override
    public String submitAwsWorkflow(WorkflowConfiguration workflowConfiguration) {
        return workflowContainerService.submitAwsWorkFlow(workflowConfiguration);
    }

    @Override
    @WithCustomerSpace
    public void stopWorkflow(String customerSpace, Long workflowId) {
        workflowService.stop(customerSpace, new WorkflowExecutionId(workflowId));
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
        job.setJobType(workflowJob.getType());

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
