package com.latticeengines.workflow.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.service.impl.WorkflowServiceImpl;

@Component("workflowExecutionCache")
public class WorkflowExecutionCache {

    private static final int MAX_CACHE_SIZE = 1000;
    private static final Log log = LogFactory.getLog(WorkflowServiceImpl.class);

    @Value("${workflow.jobs.numthreads}")
    private String numJobThreads;

    private Cache<Long, Job> cache;
    private ExecutorService executorService;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private WorkflowService workflowService;

    @PostConstruct
    public void init() {
         cache = CacheBuilder.newBuilder()
                     .maximumSize(MAX_CACHE_SIZE)
                     .build();
         executorService = Executors.newFixedThreadPool(Integer.parseInt(numJobThreads));
    }

    public List<Job> getJobs(List<WorkflowExecutionId> workflowIds) throws Exception {
        List<Job> jobs = new ArrayList<>();

        List<WorkflowExecutionId> missingJobIds = new ArrayList<>();
        for (WorkflowExecutionId workflowId : workflowIds) {
            if (cache.getIfPresent(workflowId.getId()) == null) {
                missingJobIds.add(workflowId);
            } else {
                jobs.add(cache.getIfPresent(workflowId.getId()));
            }
        }

        jobs.addAll(loadMissingJobs(missingJobIds));
        return jobs;
    }

    public Job getJob(WorkflowExecutionId workflowId) {
        if (cache.getIfPresent(workflowId.getId()) != null) {
            return cache.getIfPresent(workflowId.getId());
        }

        log.info(String.format("Job with id: %s is not in the cache, reloading.", workflowId.getId()));
        JobExecution jobExecution = jobExplorer.getJobExecution(workflowId.getId());
        JobInstance jobInstance = jobExecution.getJobInstance();
        WorkflowStatus workflowStatus = this.workflowService.getStatus(workflowId);

        Job job = new Job();
        job.setId(workflowId.getId());
        job.setJobStatus(getJobStatusFromBatchStatus(workflowStatus.getStatus()));
        job.setStartTimestamp(workflowStatus.getStartTime());
        job.setJobType(jobInstance.getJobName());
        job.setSteps(getJobSteps(jobExecution));
        if (Job.TERMINAL_JOB_STATUS.contains(job.getJobStatus())) {
            job.setEndTimestamp(workflowStatus.getEndTime());
            cache.put(job.getId(), job);
        }

        return job;
    }

    private List<Job> loadMissingJobs(List<WorkflowExecutionId> workflowIds) throws Exception {
        List<Job> missingJobs = new ArrayList<>();
        Set<Callable<Job>> callables = new HashSet<>();

        for (final WorkflowExecutionId workflowId : workflowIds) {
            callables.add(new Callable<Job>() {
                public Job call() throws Exception {
                    return getJob(workflowId);
                }
            });
        }

        List<Future<Job>> futures = executorService.invokeAll(callables);
        for (Future<Job> future : futures) {
            Job job = future.get();
            missingJobs.add(job);
        }

        return missingJobs;
    }

    private List<JobStep> getJobSteps(JobExecution jobExecution) {
        List<JobStep> steps = new ArrayList<>();

        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            JobStep jobStep = new JobStep();
            jobStep.setJobStepType(stepExecution.getStepName());
            jobStep.setStepStatus(getJobStatusFromBatchStatus(stepExecution.getStatus()));
            jobStep.setStartTimestamp(stepExecution.getStartTime());
            jobStep.setEndTimestamp(stepExecution.getEndTime());
            steps.add(jobStep);
        }

        return steps;
    }

    private JobStatus getJobStatusFromBatchStatus(BatchStatus batchStatus) {
        JobStatus jobStatus = JobStatus.PENDING;
        switch (batchStatus) {
        case UNKNOWN:
            jobStatus = JobStatus.PENDING;
            break;
        case STARTED:
        case STARTING:
        case STOPPING:
            jobStatus = JobStatus.RUNNING;
            break;
        case COMPLETED:
            jobStatus = JobStatus.COMPLETED;
            break;
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

}
