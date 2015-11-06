package com.latticeengines.workflow.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.TransformerUtils;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.WorkflowId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflow.exposed.build.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.service.WorkflowService;

@Component("workflowService")
public class WorkflowServiceImpl implements WorkflowService {

    private static final String WORKFLOW_SERVICE_UUID = "WorkflowServiceUUID";
    private static final EnumSet<BatchStatus> TERMINAL_STATUS = EnumSet.of(BatchStatus.ABANDONED,
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

    @Override
    public List<String> getNames() {
        return new ArrayList<String>(jobRegistry.getJobNames());
    }

    @Override
    public WorkflowId start(String workflowName, WorkflowConfiguration workflowConfiguration) {
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
        return new WorkflowId(jobExecution.getId());
    }

    @Override
    public WorkflowId restart(WorkflowId workflowId) {
        Long id = 0L;

        try {
            id = jobOperator.restart(workflowId.getId());
        } catch (JobInstanceAlreadyCompleteException | NoSuchJobExecutionException | NoSuchJobException
                | JobRestartException | JobParametersInvalidException e) {
            throw new LedpException(LedpCode.LEDP_28002, e, new String[] { getWorkflowName(workflowId) });
        }

        return new WorkflowId(id);
    }

    @Override
    public void stop(WorkflowId workflowId) {
        try {
            jobOperator.stop(workflowId.getId());
        } catch (NoSuchJobExecutionException | JobExecutionNotRunningException e) {
            throw new LedpException(LedpCode.LEDP_28003, e, new String[] { getWorkflowName(workflowId) });
        }
    }

    @Override
    public WorkflowStatus getStatus(WorkflowId workflowId) {
        JobExecution jobExecution = jobExplorer.getJobExecution(workflowId.getId());
        WorkflowStatus workflowStatus = new WorkflowStatus();
        workflowStatus.setStatus(jobExecution.getStatus());
        workflowStatus.setStartTime(jobExecution.getStartTime());
        workflowStatus.setEndTime(jobExecution.getEndTime());
        workflowStatus.setLastUpdated(jobExecution.getLastUpdated());

        return workflowStatus;
    }

    private String getWorkflowName(WorkflowId workflowId) {
        return jobExplorer.getJobExecution(workflowId.getId()).getJobInstance().getJobName();
    }

    @Override
    public List<String> getStepNames(WorkflowId workflowId) {
        JobExecution jobExecution = jobExplorer.getJobExecution(workflowId.getId());

        return getStepNamesFromExecution(jobExecution);
    }

    private List<String> getStepNamesFromExecution(JobExecution jobExecution) {
        @SuppressWarnings("unchecked")
        Collection<String> stepNames = CollectionUtils.collect(jobExecution.getStepExecutions(),
                TransformerUtils.invokerTransformer("getStepName"));

        return new ArrayList<String>(stepNames);
    }

    public WorkflowStatus waitForCompletion(WorkflowId workflowId) throws Exception {
        return waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT);
    }

    public WorkflowStatus waitForCompletion(WorkflowId workflowId, long maxWaitTime) throws Exception {
        WorkflowStatus status = null;
        long start = System.currentTimeMillis();

        // break label for inner loop
        done: do {
            status = getStatus(workflowId);
            if (status == null) {
                break;
            } else if (TERMINAL_STATUS.contains(status.getStatus())) {
                break done;
            }
            Thread.sleep(1000);
        } while (System.currentTimeMillis() - start < maxWaitTime);

        return status;
    }

}
