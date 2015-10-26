package com.latticeengines.workflow.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
import com.latticeengines.workflow.service.WorkflowId;
import com.latticeengines.workflow.service.WorkflowService;

@Component("workflowService")
public class WorkflowServiceImpl implements WorkflowService {

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

    // TODO start a yarn container and start workflow in it using simplejoblauncher
    @Override
    public WorkflowId start(String workflowName) {
        Job modelWorkflow = null;
        try {
            modelWorkflow = jobRegistry.getJob(workflowName);
        } catch (NoSuchJobException e1) {
            throw new LedpException(LedpCode.LEDP_28000, new String[] { workflowName });
        }
        JobParameters parms = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
                .addString("testParameter", "testValue", false).toJobParameters();

        JobExecution jobExecution = null;
        try {
            jobExecution = jobLauncher.run(modelWorkflow, parms);
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
    public BatchStatus getStatus(WorkflowId workflowId) {
        return jobExplorer.getJobExecution(workflowId.getId()).getStatus();
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

}
