package com.latticeengines.workflowapi.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithCustomerSpace;
import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
import com.latticeengines.workflow.exposed.service.JobCacheService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.exposed.util.WorkflowJobUtils;
import com.latticeengines.workflowapi.service.WorkflowContainerService;
import com.latticeengines.workflowapi.service.WorkflowJobService;

@Component("workflowApiWorkflowJobService")
public class WorkflowJobServiceImpl implements WorkflowJobService {
    private static final Logger log = LoggerFactory.getLogger(WorkflowJobServiceImpl.class);

    @Value("${hadoop.yarn.timeline-service.webapp.address}")
    private String timelineServiceUrl;

    @Value("${workflow.jobs.disableCache:false}")
    private Boolean disableCache;

    @Autowired
    private JobCacheService jobCacheService;

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

    private static final long HEARTBEAT_FAILURE_THRESHOLD = TimeUnit.MILLISECONDS.convert(10L, TimeUnit.MINUTES);

    private static final long SPRING_BATCH_FAILURE_THRESHOLD = TimeUnit.MILLISECONDS.convert(1L, TimeUnit.HOURS);

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
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowPidsOrTypesOrParentJobId(workflowPids, null,
                null);
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
        return WorkflowJobUtils.assembleJob(reportService, leJobExecutionRetriever, timelineServiceUrl, workflowJob,
                includeDetails);
    }

    @Override
    @WithCustomerSpace
    public Job getJobByWorkflowIdFromCache(String customerSpace, @NotNull Long workflowId, boolean includeDetails) {
        if (disableCache) {
            return getJobByWorkflowId(customerSpace, workflowId, includeDetails);
        }
        Job job = jobCacheService.getByWorkflowId(workflowId, includeDetails);
        checkLastUpdateTime(Collections.singletonList(toWorkflowJob(job)));
        if (!currentTenantHasAccess(job)) {
            return null;
        }
        removeTenantInfo(job);
        return job;
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
        return WorkflowJobUtils.assembleJob(reportService, leJobExecutionRetriever, timelineServiceUrl, workflowJob,
                includeDetails);
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
        return WorkflowJobUtils.assembleJob(reportService, leJobExecutionRetriever, timelineServiceUrl, workflowJob,
                includeDetails);
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobsByCustomerSpace(String customerSpace, Boolean includeDetails) {
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findAll();
        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkExecutionId(workflowJobs);
        workflowJobs = checkLastUpdateTime(workflowJobs);
        List<Job> jobs = workflowJobs.stream().map(workflowJob -> WorkflowJobUtils.assembleJob(reportService,
                leJobExecutionRetriever, timelineServiceUrl, workflowJob, includeDetails)).collect(Collectors.toList());
        jobs.forEach(this::removeTenantInfo);
        return jobs;
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobsByCustomerSpaceFromCache(String customerSpace, Boolean includeDetails) {
        if (disableCache) {
            return getJobsByCustomerSpace(customerSpace, includeDetails);
        }
        try {
            List<Job> jobs = jobCacheService.getByTenant(MultiTenantContext.getTenant(), includeDetails);
            checkExecutionId(jobs.stream().map(this::toWorkflowJob).collect(Collectors.toList()));
            checkLastUpdateTime(jobs.stream().map(this::toWorkflowJob).collect(Collectors.toList()));
            jobs.forEach(this::removeTenantInfo);
            return jobs;
        } catch (Exception e) {
            log.error(String.format("Failed to retrieve jobs from cache for customer space %s, fallback to database",
                    customerSpace), e);
            // fallback
            return getJobsByCustomerSpace(customerSpace, includeDetails);
        }
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobsByWorkflowIds(String customerSpace, List<Long> workflowIds, List<String> types,
            Boolean includeDetails, Boolean hasParentId, Long parentJobId) {
        return getJobsByWorkflowIds(customerSpace, workflowIds, types, null, includeDetails, hasParentId, parentJobId);
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobsByWorkflowIds(String customerSpace, List<Long> workflowIds, List<String> types,
            List<String> jobStatuses, Boolean includeDetails, Boolean hasParentId, Long parentJobId) {
        Optional<List<Long>> optionalWorkflowIds = Optional.ofNullable(workflowIds);
        Optional<List<String>> optionalTypes = Optional.ofNullable(types);

        // JobStatuses
        List<String> workflowStatuses = WorkflowJobUtils.getWorkflowJobMappingsForJobStatuses(jobStatuses);

        List<WorkflowJob> workflowJobs;

        if (hasParentId != null && hasParentId) {
            workflowJobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(optionalWorkflowIds.orElse(null),
                    optionalTypes.orElse(null), parentJobId);
        } else {
            workflowJobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(optionalWorkflowIds.orElse(null),
                    optionalTypes.orElse(null), workflowStatuses, null);
        }

        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkLastUpdateTime(workflowJobs);

        // Apply the status filter again on results, Because "checkLastUpdateTime" can
        // update the status
        if (CollectionUtils.isNotEmpty(jobStatuses) && CollectionUtils.isNotEmpty(workflowJobs)) {
            Set<String> jobStatusSet = jobStatuses.stream().map(st -> st.toUpperCase()).collect(Collectors.toSet());
            workflowJobs = workflowJobs.stream().filter(workflowJob -> {
                return workflowJob.getStatus() == null || jobStatusSet.contains(workflowJob.getStatus().toUpperCase());
            }).collect(Collectors.toList());
        }

        return workflowJobs.stream().map(workflowJob -> WorkflowJobUtils.assembleJob(reportService,
                leJobExecutionRetriever, timelineServiceUrl, workflowJob, includeDetails)).collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobsByWorkflowIdsFromCache(String customerSpace, @NotNull List<Long> workflowIds,
            boolean includeDetails) {
        if (disableCache) {
            return getJobsByWorkflowIds(customerSpace, workflowIds, null, includeDetails, false, -1L);
        }
        List<Job> jobs = jobCacheService.getByWorkflowIds(workflowIds, includeDetails);
        checkLastUpdateTime(jobs.stream().map(this::toWorkflowJob).collect(Collectors.toList()));
        return jobs.stream().filter(this::currentTenantHasAccess).collect(Collectors.toList());
    }

    @Override
    @WithCustomerSpace
    public List<Job> getJobsByWorkflowPids(String customerSpace, List<Long> workflowPids, List<String> types,
            Boolean includeDetails, Boolean hasParentId, Long parentJobId) {
        Optional<List<Long>> optionalWorkflowPids = Optional.ofNullable(workflowPids);
        Optional<List<String>> optionalTypes = Optional.ofNullable(types);
        List<WorkflowJob> workflowJobs;

        if (hasParentId != null && hasParentId) {
            workflowJobs = workflowJobEntityMgr.findByWorkflowPidsOrTypesOrParentJobId(
                    optionalWorkflowPids.orElse(null), optionalTypes.orElse(null), parentJobId);
        } else {
            workflowJobs = workflowJobEntityMgr.findByWorkflowPidsOrTypesOrParentJobId(
                    optionalWorkflowPids.orElse(null), optionalTypes.orElse(null), null);
        }

        workflowJobs.removeIf(Objects::isNull);
        workflowJobs = checkExecutionId(workflowJobs);
        workflowJobs = checkLastUpdateTime(workflowJobs);

        return workflowJobs.stream().map(workflowJob -> WorkflowJobUtils.assembleJob(reportService,
                leJobExecutionRetriever, timelineServiceUrl, workflowJob, includeDetails)).collect(Collectors.toList());
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
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowPidsOrTypesOrParentJobId(workflowPids, null,
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
        long workflowId = workflowJob != null ? workflowJob.getWorkflowId() : -1L;
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

            long currentTimeMillis = System.currentTimeMillis();
            if ((currentTimeMillis - workflowJob.getStartTimeInMillis()) > SPRING_BATCH_FAILURE_THRESHOLD
                    && workflowJob.getWorkflowId() == null) {
                workflowJob.setStatus(JobStatus.FAILED.name());
                workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);
                log.warn(String.format(
                        "Spring-batch has failed to start job. WorkflowPId=%s. Job started at %s. "
                                + "Current timestamp=%s. Spring-batch failure threshold=%s. "
                                + "DiffBetweenCurrentAndStartTime=%s",
                        workflowJob.getPid(), workflowJob.getStartTimeInMillis(), currentTimeMillis,
                        SPRING_BATCH_FAILURE_THRESHOLD, currentTimeMillis - workflowJob.getStartTimeInMillis()));
                if (workflowJob.getWorkflowId() != null) {
                    // invalidate cache entry
                    jobCacheService.evictByWorkflowIds(Collections.singletonList(workflowJob.getWorkflowId()));
                }
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
                log.warn("WorkflowJob is null. Skip checking lastUpdateTime.");
                continue;
            }

            if (JobStatus.fromString(workflowJob.getStatus()).isTerminated()) {
                continue;
            }

            long currentTimeMillis = System.currentTimeMillis();
            WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowJob.getPid());
            /*
             * Fail the job because the last heartbeat is received over HEARTBEAT_FAILURE_THRESHOLD ago.
             *
             * NOTE:
             * 1. Initially, createTime == lastUpdateTime
             * 2. lastUpdateTime != createTime is used to make sure we only fail jobs that have sent at least one
             * heartbeat. DO NOT CHANGE THIS because if there are a lot of jobs in queue, currentTime - lastUpdateTime
             * can be greater than the threshold but the job is not actually failed (hasn't even started yet).
             *
             * TODO have a better way to determine whether the first heartbeat has been sent
             */
            if (jobUpdate != null && jobUpdate.getLastUpdateTime() != null
                    && !jobUpdate.getLastUpdateTime().equals(jobUpdate.getCreateTime())
                    && (currentTimeMillis - jobUpdate.getLastUpdateTime()) > HEARTBEAT_FAILURE_THRESHOLD) {
                // Before failing the job, check spring batch status first. If there is no spring-batch associated, or
                // spring-batch gives unsuccessful status, we fail the job.
                WorkflowStatus status = workflowService.getStatus(new WorkflowExecutionId(workflowJob.getWorkflowId()));
                if (status == null || status.getStatus().isUnsuccessful()) {
                    workflowJob.setStatus(JobStatus.FAILED.name());
                    workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);
                    log.warn(String.format(
                            "Heartbeat failure threshold exceeded, failing the job. "
                                    + "WorkflowId=%s. Heartbeat created time=%s. "
                                    + "Heartbeat update time=%s. Heartbeat failure threshold=%s. Current time=%s. "
                                    + "DiffBetweenLastUpdateAndCreate=%s. DiffBetweenCurrentAndLastUpdate=%s",
                            workflowJob.getWorkflowId(), jobUpdate.getCreateTime(), jobUpdate.getLastUpdateTime(),
                            HEARTBEAT_FAILURE_THRESHOLD, currentTimeMillis,
                            jobUpdate.getLastUpdateTime() - jobUpdate.getCreateTime(),
                            currentTimeMillis - jobUpdate.getLastUpdateTime()));
                    if (workflowJob.getWorkflowId() != null) {
                        // invalidate cache entry
                        jobCacheService.evictByWorkflowIds(Collections.singletonList(workflowJob.getWorkflowId()));
                    }
                }
            }
        }

        return workflowJobs;
    }

    /*
     * determine whether current tenant has access to the target job
     */
    private boolean currentTenantHasAccess(Job job) {
        if (MultiTenantContext.getTenant() == null || job == null) {
            return true;
        }

        Tenant tenant = MultiTenantContext.getTenant();
        if (job.getTenantId() != null && !job.getTenantId().equals(tenant.getId())) {
            return false;
        }
        if (job.getTenantPid() != null && !job.getTenantPid().equals(tenant.getPid())) {
            return false;
        }

        return true;
    }

    /*
     * remove tenant info added to the cache entry for authorization
     */
    private void removeTenantInfo(Job job) {
        if (job == null) {
            return;
        }

        job.setTenantPid(null);
        job.setTenantId(null);
    }

    /*
     * transform job to workflowjob for last update time check
     */
    private WorkflowJob toWorkflowJob(@NotNull Job job) {
        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setPid(job.getPid());
        workflowJob.setWorkflowId(job.getId());
        workflowJob.setApplicationId(job.getApplicationId());
        if (job.getStartTimestamp() != null) {
            workflowJob.setStartTimeInMillis(job.getStartTimestamp().getTime());
        }
        workflowJob.setStatus(job.getJobStatus() == null ? null : job.getJobStatus().name());
        return workflowJob;
    }

    @VisibleForTesting
    void setWorkflowService(WorkflowService workflowService) {
        this.workflowService = workflowService;
    }

    @VisibleForTesting
    void setLeJobExecutionRetriever(LEJobExecutionRetriever leJobExecutionRetriever) {
        this.leJobExecutionRetriever = leJobExecutionRetriever;
    }

}
