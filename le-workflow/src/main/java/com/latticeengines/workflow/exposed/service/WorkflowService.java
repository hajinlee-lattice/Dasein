package com.latticeengines.workflow.exposed.service;

import java.util.List;
import java.util.Map;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.context.ApplicationContext;

import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowInstanceId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowService {

    WorkflowExecutionId start(WorkflowConfiguration workflowConfiguration);

    WorkflowExecutionId restart(WorkflowExecutionId workflowId, WorkflowJob workflowJob);

    WorkflowExecutionId restart(WorkflowInstanceId workflowId, WorkflowJob workflowJob);

    void stop(WorkflowExecutionId workflowId);

    WorkflowStatus getStatus(WorkflowExecutionId workflowId);

    WorkflowStatus getStatus(WorkflowExecutionId workflowId, JobExecution jobExecution);

    JobStatus getJobStatus(WorkflowExecutionId workflowId);

    List<String> getStepNames(WorkflowExecutionId workflowId);

    WorkflowStatus waitForCompletion(WorkflowExecutionId workflowId) throws Exception;

    WorkflowStatus waitForCompletion(WorkflowExecutionId workflowId, long maxWaitTime) throws Exception;

    void sleepForCompletion(WorkflowExecutionId workflowId);

    WorkflowStatus waitForCompletion(WorkflowExecutionId workflowId, long maxWaitTime, long checkInterval)
            throws Exception;

    long startWorkflowJob(WorkflowConfiguration workflowConfiguration);

    Map<String, String> getInputs(Map<String, String> inputContext);

    JobParameters createJobParams(WorkflowConfiguration workflowConfiguratio);

    WorkflowExecutionId start(WorkflowConfiguration workflowConfiguration, WorkflowJob workflowJob);

    <T extends WorkflowConfiguration> void registerJob(T workflowConfig, ApplicationContext context);

    void unRegisterJob(String workflowName);

    JobStatus sleepForCompletionWithStatus(WorkflowExecutionId workflowId);

}
