package com.latticeengines.workflow.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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

@Component("workflowExecutionCache")
public class WorkflowExecutionCache {

    private static final int MAX_CACHE_SIZE = 100000;
    private static final Log log = LogFactory.getLog(WorkflowExecutionCache.class);

    @Value("${workflow.jobs.numthreads}")
    private String numJobThreads;

    private Cache<Long, Job> cache;
    private ExecutorService executorService;

    @Autowired
    private LEJobExecutionRetriever leJobExecutionRetriever;

    @Autowired
    private JobProxy jobProxy;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private ReportService reportService;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @PostConstruct
    public void init() {
        cache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build();
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
        return clearJobDetails(jobs);
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

            nonDetailedJobs.add(nonDetailedJob);
        }

        return nonDetailedJobs;
    }

    public Job getJob(WorkflowExecutionId workflowId) {
        if (cache.getIfPresent(workflowId.getId()) != null) {
            return cache.getIfPresent(workflowId.getId());
        }

        log.info(String.format("Job with id: %s is not in the cache, reloading.",
                workflowId.getId()));

        try {
            JobExecution jobExecution = leJobExecutionRetriever.getJobExecution(workflowId.getId());
            JobInstance jobInstance = jobExecution.getJobInstance();
            WorkflowStatus workflowStatus = workflowService.getStatus(workflowId);
            WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(workflowId.getId());

            Job job = new Job();
            job.setId(workflowId.getId());
            job.setJobStatus(getJobStatusFromBatchStatus(workflowStatus.getStatus()));
            job.setStartTimestamp(workflowStatus.getStartTime());
            job.setJobType(jobInstance.getJobName());
            job.setSteps(getJobSteps(jobExecution));
            job.setReports(getReports(workflowJob));
            job.setOutputs(getOutputs(workflowJob));
            if (workflowJob != null) {
                job.setInputs(workflowJob.getInputContext());
                job.setApplicationId(workflowJob.getApplicationId());
                job.setUser(workflowJob.getUserId());
                ErrorDetails errorDetails = workflowJob.getErrorDetails();
                if (errorDetails != null) {
                    job.setErrorCode(errorDetails.getErrorCode());
                    job.setErrorMsg(errorDetails.getErrorMsg());
                }
            }

            if (Job.TERMINAL_JOB_STATUS.contains(job.getJobStatus())) {
                job.setEndTimestamp(workflowStatus.getEndTime());
                cache.put(job.getId(), job);
            }
            return job;
        } catch (Exception e) {
            log.error(
                    String.format("Getting job status for workflow: %d failed", workflowId.getId()),
                    e);
            throw e;
        }
    }

    private List<Job> loadMissingJobs(List<WorkflowExecutionId> workflowIds) throws Exception {
        List<Job> missingJobs = new ArrayList<>();
        Set<Callable<Job>> callables = new HashSet<>();

        for (final WorkflowExecutionId workflowId : workflowIds) {
            callables.add(new Callable<Job>() {
                @Override
                public Job call() throws Exception {
                    try {
                        return getJob(workflowId);
                    } catch (Exception e) {
                        return null;
                    }
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

    public List<JobStep> getJobSteps(JobExecution jobExecution) {
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

}
