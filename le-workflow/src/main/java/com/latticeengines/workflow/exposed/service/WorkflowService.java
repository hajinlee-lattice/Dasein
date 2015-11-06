package com.latticeengines.workflow.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.workflow.WorkflowId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflow.exposed.build.WorkflowConfiguration;

public interface WorkflowService {

    List<String> getNames();

    WorkflowId start(String workflowName, WorkflowConfiguration workflowConfiguration);

    WorkflowId restart(WorkflowId workflowId);

    void stop(WorkflowId workflowId);

    WorkflowStatus getStatus(WorkflowId workflowId);

    List<String> getStepNames(WorkflowId workflowId);

    WorkflowStatus waitForCompletion(WorkflowId workflowId) throws Exception;

    WorkflowStatus waitForCompletion(WorkflowId workflowId, long maxWaitTime) throws Exception;

}
