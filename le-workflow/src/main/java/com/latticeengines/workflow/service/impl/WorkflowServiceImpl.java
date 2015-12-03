package com.latticeengines.workflow.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.TransformerUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.launch.NoSuchJobInstanceException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.WorkflowAppContext;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowInstanceId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.workflow.exposed.entitymgr.WorkflowAppContextEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;

@Component("workflowService")
public class WorkflowServiceImpl implements WorkflowService {

    private static final Log log = LogFactory.getLog(WorkflowServiceImpl.class);
    private static final String WORKFLOW_SERVICE_UUID = "WorkflowServiceUUID";
    private static final EnumSet<BatchStatus> TERMINAL_BATCH_STATUS = EnumSet.of(BatchStatus.ABANDONED,
            BatchStatus.COMPLETED, BatchStatus.FAILED, BatchStatus.STOPPED);
    private static final long MAX_MILLIS_TO_WAIT = 1000L * 60 * 60 * 24;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRegistry jobRegistry;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private WorkflowAppContextEntityMgr workflowAppContextEntityMgr;

    @Override
    public List<String> getNames() {
        return new ArrayList<String>(jobRegistry.getJobNames());
    }

    @Override
    public WorkflowExecutionId start(String workflowName, WorkflowConfiguration workflowConfiguration) {
        Job workflow = null;
        try {
            workflow = jobRegistry.getJob(workflowName);
        } catch (NoSuchJobException e1) {
            throw new LedpException(LedpCode.LEDP_28000, new String[] { workflowName });
        }

        JobParametersBuilder parmsBuilder = new JobParametersBuilder().addString(WORKFLOW_SERVICE_UUID, UUID
                .randomUUID().toString());
        if (workflowConfiguration != null) {
            for (String configurationClassName : workflowConfiguration.getConfigRegistry().keySet()) {
                parmsBuilder.addString(configurationClassName,
                        workflowConfiguration.getConfigRegistry().get(configurationClassName));
            }
        }

        JobParameters parms = parmsBuilder.toJobParameters();
        JobExecution jobExecution = null;
        try {
            jobExecution = jobLauncher.run(workflow, parms);
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
                | JobParametersInvalidException e) {
            throw new LedpException(LedpCode.LEDP_28001, e, new String[] { workflowName });
        }

        if (workflowConfiguration != null && workflowConfiguration.getCustomerSpace() != null) {
            Tenant tenant = tenantEntityMgr.findByTenantId(workflowConfiguration.getCustomerSpace().toString());
            if (tenant != null) {
                WorkflowAppContext workflowAppContext = new WorkflowAppContext();
                workflowAppContext.setTenant(tenant);
                workflowAppContext.setWorkflowId(jobExecution.getId());
                workflowAppContextEntityMgr.create(workflowAppContext);
            }
        }

        return new WorkflowExecutionId(jobExecution.getId());
    }

    @Override
    public WorkflowExecutionId restart(WorkflowInstanceId workflowInstanceId) {
        Long jobExecutionId = -1L;
        List<Long> jobExecutions;
        try {
            jobExecutions = jobOperator.getExecutions(workflowInstanceId.getId());
        } catch (NoSuchJobInstanceException e) {
            throw new LedpException(LedpCode.LEDP_28002, e, new String[] { String.valueOf(workflowInstanceId.getId()),
                    String.valueOf(workflowInstanceId.getId()) });
        }

        jobExecutionId = jobExecutions.get(0);
        log.info(String.format("Restarting workflowId:%d from most recent jobExecutionId:%d.",
                workflowInstanceId.getId(), jobExecutionId));
        return restart(new WorkflowExecutionId(jobExecutionId));
    }

    @Override
    public WorkflowExecutionId restart(WorkflowExecutionId workflowExecutionId) {
        Long id = 0L;

        try {
            id = jobOperator.restart(workflowExecutionId.getId());
            log.info(String.format("Restarted workflow from jobExecutionId:%d. Created new jobExecutionId:%d",
                    workflowExecutionId.getId(), id));
        } catch (JobInstanceAlreadyCompleteException | NoSuchJobExecutionException | NoSuchJobException
                | JobRestartException | JobParametersInvalidException e) {
            throw new LedpException(LedpCode.LEDP_28002, e, new String[] { String.valueOf(workflowExecutionId.getId()),
                    String.valueOf(workflowExecutionId.getId()) });
        }

        return new WorkflowExecutionId(id);
    }

    @Override
    public void stop(WorkflowExecutionId workflowId) {
        try {
            jobOperator.stop(workflowId.getId());
        } catch (NoSuchJobExecutionException | JobExecutionNotRunningException e) {
            throw new LedpException(LedpCode.LEDP_28003, e, new String[] { getWorkflowName(workflowId) });
        }
    }

    @Override
    public WorkflowStatus getStatus(WorkflowExecutionId workflowId) {
        JobExecution jobExecution = jobExplorer.getJobExecution(workflowId.getId());
        WorkflowStatus workflowStatus = new WorkflowStatus();
        workflowStatus.setStatus(jobExecution.getStatus());
        workflowStatus.setStartTime(jobExecution.getStartTime());
        workflowStatus.setEndTime(jobExecution.getEndTime());
        workflowStatus.setLastUpdated(jobExecution.getLastUpdated());

        return workflowStatus;
    }

    @Override
    public com.latticeengines.domain.exposed.workflow.Job getJob(WorkflowExecutionId workflowId) {
        JobExecution jobExecution = jobExplorer.getJobExecution(workflowId.getId());
        JobInstance jobInstance = jobExecution.getJobInstance();
        WorkflowStatus workflowStatus = getStatus(workflowId);

        com.latticeengines.domain.exposed.workflow.Job job = new com.latticeengines.domain.exposed.workflow.Job();
        job.setId(workflowId.getId());
        job.setJobStatus(getJobStatusFromBatchStatus(workflowStatus.getStatus()));
        job.setStartTimestamp(workflowStatus.getStartTime());
        if (job.getJobStatus() == JobStatus.CANCELLED || job.getJobStatus() == JobStatus.COMPLETED
                || job.getJobStatus() == JobStatus.FAILED) {
            job.setEndTimestamp(workflowStatus.getEndTime());
        }

        job.setJobType(jobInstance.getJobName());

        job.setSteps(getJobSteps(jobExecution));

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

    private String getWorkflowName(WorkflowExecutionId workflowId) {
        return jobExplorer.getJobExecution(workflowId.getId()).getJobInstance().getJobName();
    }

    @Override
    public List<String> getStepNames(WorkflowExecutionId workflowId) {
        JobExecution jobExecution = jobExplorer.getJobExecution(workflowId.getId());

        return getStepNamesFromExecution(jobExecution);
    }

    private List<String> getStepNamesFromExecution(JobExecution jobExecution) {
        @SuppressWarnings("unchecked")
        Collection<String> stepNames = CollectionUtils.collect(jobExecution.getStepExecutions(),
                TransformerUtils.invokerTransformer("getStepName"));

        return new ArrayList<String>(stepNames);
    }

    @Override
    public WorkflowStatus waitForCompletion(WorkflowExecutionId workflowId) throws Exception {
        return waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT);
    }

    @Override
    public WorkflowStatus waitForCompletion(WorkflowExecutionId workflowId, long maxWaitTime) throws Exception {
        WorkflowStatus status = null;
        long start = System.currentTimeMillis();

        // break label for inner loop
        done: do {
            status = getStatus(workflowId);
            if (status == null) {
                break;
            } else if (TERMINAL_BATCH_STATUS.contains(status.getStatus())) {
                break done;
            }
            Thread.sleep(1000);
        } while (System.currentTimeMillis() - start < maxWaitTime);

        return status;
    }

}
