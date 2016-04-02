package com.latticeengines.pls.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.pls.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowJobService {

    void create(WorkflowJob workflowJob);

    List<WorkflowJob> getAll();

    WorkflowStatus getWorkflowStatusFromApplicationId(String appId);

    ApplicationId submit(WorkflowConfiguration configuration);

}
