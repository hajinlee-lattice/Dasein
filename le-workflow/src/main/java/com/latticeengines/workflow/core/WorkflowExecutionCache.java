package com.latticeengines.workflow.core;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.ReportService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.exposed.util.WorkflowUtils;

@Component("workflowExecutionCache")
public class WorkflowExecutionCache {

    private static final int MAX_CACHE_SIZE = 100000;
    private static final Logger log = LoggerFactory.getLogger(WorkflowExecutionCache.class);

    @Value("${workflow.jobs.numthreads}")
    private String numJobThreads;

    @Value("${workflow.jobs.maxDynamicCacheSize:300}")
    private String maxDynamicCacheSize;

    @Value("${workflow.jobs.expireTime:60}")
    private String expireTime;

    private Cache<Long, Job> dynamicCache;
    private Cache<Long, Job> staticCache;
    private ExecutorService executorService;

    @Autowired
    private LEJobExecutionRetriever leJobExecutionRetriever;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private ReportService reportService;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private JobProxy jobProxy;

    @PostConstruct
    public void init() {
        dynamicCache = Caffeine.newBuilder()
                .maximumSize(Long.parseLong(maxDynamicCacheSize))
                .expireAfterWrite(Long.parseLong(expireTime), TimeUnit.SECONDS)
                .removalListener((Long key, Job value, RemovalCause cause) -> {
                    log.info(String.format("Job %d is removed from dynamic cache. Cause: %s.",
                            key, cause));
                })
                .build();

        staticCache = Caffeine.newBuilder()
                .maximumSize(MAX_CACHE_SIZE - Long.parseLong(maxDynamicCacheSize))
                .removalListener((Long key, Job value, RemovalCause cause) -> {
                    log.info(String.format("Job %d is removed from static cache. Cause: %s.",
                            key, cause));
                })
                .build();

        executorService = Executors.newFixedThreadPool(Integer.parseInt(numJobThreads));
    }

    public List<Job> getJobs(List<WorkflowExecutionId> workflowIds) throws Exception {
        List<Job> jobs = new ArrayList<>();

        List<WorkflowExecutionId> missingJobIds = new ArrayList<>();
        for (WorkflowExecutionId workflowId : workflowIds) {
            Job job = getIfPresent(workflowId);
            if (job != null) {
                jobs.add(job);
            } else {
                missingJobIds.add(workflowId);
            }
        }

        jobs.addAll(loadMissingJobs(missingJobIds));
        return clearJobDetails(jobs);
    }

    public Job getJob(WorkflowExecutionId workflowId) {
        Job job = getIfPresent(workflowId);
        if (job != null) {
            return job;
        }

        job = queryJob(workflowId.getId());
        if (job != null) {
            if (job.getJobStatus().isTerminated()) {
                staticCache.put(workflowId.getId(), job);
            } else {
                dynamicCache.put(workflowId.getId(), job);
            }
        }

        return job;
    }

    private Job queryJob(Long jobId) {
        log.info(String.format("Job with id: %s is not in the cache, reloading.", jobId));
        try {
            JobExecution jobExecution = leJobExecutionRetriever.getJobExecution(jobId);
            if (jobExecution == null) {
                return null;
            }

            JobInstance jobInstance = jobExecution.getJobInstance();
            WorkflowExecutionId workflowId = new WorkflowExecutionId(jobId);
            WorkflowStatus workflowStatus = workflowService.getStatus(workflowId);
            WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(jobId);

            Job job = new Job();
            job.setId(jobId);
            if (FinalApplicationStatus.FAILED.equals(workflowJob.getStatus())) {
                job.setJobStatus(JobStatus.FAILED);
            } else {
                JobStatus status = getJobStatusFromBatchStatus(workflowStatus.getStatus());
                if (status.isTerminated()) {
                    job.setJobStatus(status);
                } else {
                    WorkflowUtils.updateJobFromYarn(job, workflowJob, jobProxy, workflowJobEntityMgr);
                }
            }

            job.setStartTimestamp(workflowStatus.getStartTime());
            job.setJobType(jobInstance.getJobName());
            job.setSteps(getJobSteps(jobExecution));

            if (workflowJob.getStartTimeInMillis() != null) {
                job.setStartTimestamp(new Date(workflowJob.getStartTimeInMillis()));
            }
            job.setReports(getReports(workflowJob));
            job.setOutputs(getOutputs(workflowJob));
            job.setInputs(workflowJob.getInputContext());
            job.setApplicationId(workflowJob.getApplicationId());
            job.setUser(workflowJob.getUserId());
            ErrorDetails errorDetails = workflowJob.getErrorDetails();
            if (errorDetails != null) {
                job.setErrorCode(errorDetails.getErrorCode());
                job.setErrorMsg(errorDetails.getErrorMsg());
            }
            job.setEndTimestamp(workflowStatus.getEndTime());

            return job;
        } catch (Exception e) {
            log.error(String.format("Getting job status for workflow: %d failed", jobId), e);
            throw e;
        }
    }

    private List<Job> loadMissingJobs(List<WorkflowExecutionId> workflowIds) throws Exception {
        List<Job> missingJobs = new ArrayList<>();
        Set<Callable<Job>> callables = new HashSet<>();

        for (final WorkflowExecutionId workflowId : workflowIds) {
            callables.add(() -> {
                try {
                    return getJob(workflowId);
                } catch (Exception e) {
                    return null;
                }
            });
        }

        List<Future<Job>> futures = executorService.invokeAll(callables);
        for (Future<Job> future : futures) {
            Job job = future.get();
            if (job != null) {
                missingJobs.add(job);
            }
        }

        return missingJobs;
    }

    private List<Job> clearJobDetails(List<Job> jobs) {
        List<Job> nonDetailedJobs = new ArrayList<>();

        for (Job job : jobs) {
            Job nonDetailedJob = new Job();

            nonDetailedJob.setId(job.getId());
            nonDetailedJob.setJobStatus(job.getJobStatus());
            nonDetailedJob.setStartTimestamp(job.getStartTimestamp());
            nonDetailedJob.setJobType(job.getJobType());
            nonDetailedJob.setInputs(job.getInputs());
            nonDetailedJob.setOutputs(job.getOutputs());

            nonDetailedJobs.add(nonDetailedJob);
        }

        return nonDetailedJobs;
    }

    private List<JobStep> getJobSteps(JobExecution jobExecution) {
        List<JobStep> steps = new ArrayList<>();

        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            JobStep jobStep = new JobStep();
            jobStep.setJobStepType(stepExecution.getStepName());
            jobStep.setStepStatus(getJobStatusFromBatchStatus(stepExecution.getStatus()));
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

    private JobStatus getJobStatusFromBatchStatus(BatchStatus batchStatus) {
        JobStatus jobStatus = JobStatus.PENDING;
        switch (batchStatus) {
            case UNKNOWN:
                jobStatus = JobStatus.PENDING;
                break;
            case STARTED:
            case STARTING:
                jobStatus = JobStatus.RUNNING;
                break;
            case COMPLETED:
                jobStatus = JobStatus.COMPLETED;
                break;
            case STOPPING:
            case STOPPED:
                jobStatus = JobStatus.CANCELLED;
                break;
            case ABANDONED:
            case FAILED:
                jobStatus = JobStatus.FAILED;
                break;
        }

        return jobStatus;
    }

    private Job getIfPresent(WorkflowExecutionId workflowId) {
        Job job = staticCache.getIfPresent(workflowId.getId());
        if (job != null) {
            return job;
        }

        return dynamicCache.getIfPresent(workflowId.getId());
    }

    @VisibleForTesting
    void setWorkflowService(WorkflowService workflowService) {
        this.workflowService = workflowService;
    }

    @VisibleForTesting
    void setLEJobExecutionRetriever(LEJobExecutionRetriever leJobExecutionRetriever) {
        this.leJobExecutionRetriever = leJobExecutionRetriever;
    }

    @VisibleForTesting
    void setJobProxy(JobProxy proxy) {
        this.jobProxy = proxy;
    }

    @VisibleForTesting
    long staticCacheSize() {
        return staticCache.estimatedSize();
    }

    @VisibleForTesting
    long dynamicCacheSize() {
        return dynamicCache.estimatedSize();
    }

    @VisibleForTesting
    void cleanUp() {
        dynamicCache.cleanUp();
    }
}
