package com.latticeengines.workflowapi.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.latticeengines.workflow.exposed.util.WorkflowJobUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.workflow.annotation.WithCustomerSpace;
import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.service.WorkflowContainerService;
import com.latticeengines.workflowapi.service.WorkflowJobService;

@Component("workflowApiWorkflowJobService")
public class WorkflowJobServiceImpl implements WorkflowJobService {
    private static final Logger log = LoggerFactory.getLogger(WorkflowJobServiceImpl.class);

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
            TimeUnit.MILLISECONDS.convert(1L, TimeUnit.HOURS);

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
                null, null);
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
        return WorkflowJobUtils.assembleJob(
                reportService, leJobExecutionRetriever, timelineServiceUrl, workflowJob, includeDetails);
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
        return WorkflowJobUtils.assembleJob(
                reportService, leJobExecutionRetriever, timelineServiceUrl, workflowJob, includeDetails);
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
        return WorkflowJobUtils.assembleJob(
                reportService, leJobExecutionRetriever, timelineServiceUrl, workflowJob, includeDetails);
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobsByCustomerSpace(String customerSpace, Boolean includeDetails) {
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findAll();
        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkExecutionId(workflowJobs);
        workflowJobs = checkLastUpdateTime(workflowJobs);
        return workflowJobs.stream().map(workflowJob -> WorkflowJobUtils.assembleJob(
                reportService, leJobExecutionRetriever, timelineServiceUrl, workflowJob, includeDetails))
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

        return workflowJobs.stream().map(workflowJob -> WorkflowJobUtils.assembleJob(
                reportService, leJobExecutionRetriever, timelineServiceUrl, workflowJob, includeDetails))
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

        return workflowJobs.stream().map(workflowJob -> WorkflowJobUtils.assembleJob(
                reportService, leJobExecutionRetriever, timelineServiceUrl, workflowJob, includeDetails))
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
