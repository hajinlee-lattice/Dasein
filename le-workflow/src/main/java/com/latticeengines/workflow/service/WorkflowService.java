package com.latticeengines.workflow.service;

import java.util.List;

import org.springframework.batch.core.BatchStatus;

public interface WorkflowService {

    List<String> getNames();

    WorkflowId start(String workflowName);

    WorkflowId restart(WorkflowId workflowId);

    void stop(WorkflowId workflowId);

    BatchStatus getStatus(WorkflowId workflowId);

    List<String> getStepNames(WorkflowId workflowId);
}
