package com.latticeengines.workflow.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.TransformerUtils;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflow.exposed.service.WorkflowService;

@Component("workflowExecutionCache")
public class WorkflowExecutionCache {

    @Value("${db.workflow.jobs.numthreads}")
    private String numJobThreads;

    private static final ConcurrentMapCache cache = new ConcurrentMapCache("workflowExecutionCache");
    private ExecutorService executorService;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRegistry jobRegistry;

    @Autowired
    private JobOperator jobOperator;
    
    @Autowired
    private WorkflowService workflowService;

    public List<Job> getJobs(List<WorkflowExecutionId> workflowIds) throws Exception {
        List<Job> jobs = new ArrayList<>();

        List<WorkflowExecutionId> missingJobIds = new ArrayList<>();
        for (WorkflowExecutionId workflowId : workflowIds) {
            Cache.ValueWrapper jobValueWrapper = cache.get(workflowId);
            
            if (jobValueWrapper == null) {
                missingJobIds.add(workflowId);
            } else {
                jobs.add((Job) jobValueWrapper.get());
            }
        }
        
        jobs.addAll(loadMissingJobs(missingJobIds));

        return jobs;
    }
    
    private List<Job> loadMissingJobs(List<WorkflowExecutionId> workflowIds) throws Exception {
        if (executorService == null) {
             executorService = Executors.newFixedThreadPool(Integer.parseInt(numJobThreads));
        }

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
            cache.put(job.getId(), job);
            missingJobs.add(job);
        }

        return missingJobs;
    }
    
    public Job getJob(WorkflowExecutionId workflowId) {
        Cache.ValueWrapper jobValueWrapper = cache.get(workflowId);
        Job job = new Job();
        
        if (jobValueWrapper != null) {
            job = (Job) jobValueWrapper.get();
        } else {
            JobExecution jobExecution = jobExplorer.getJobExecution(workflowId.getId());
            JobInstance jobInstance = jobExecution.getJobInstance();
            WorkflowStatus workflowStatus = this.workflowService.getStatus(workflowId);

            job.setId(workflowId.getId());
            job.setJobStatus(getJobStatusFromBatchStatus(workflowStatus.getStatus()));
            job.setStartTimestamp(workflowStatus.getStartTime());
            if (job.getJobStatus() == JobStatus.CANCELLED || job.getJobStatus() == JobStatus.COMPLETED
                    || job.getJobStatus() == JobStatus.FAILED) {
                job.setEndTimestamp(workflowStatus.getEndTime());
            }

            job.setJobType(jobInstance.getJobName());

            job.setSteps(getJobSteps(jobExecution));

            cache.put(workflowId, job);
        }

        return job;
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
