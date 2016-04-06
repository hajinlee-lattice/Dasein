package com.latticeengines.pls.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowJobService {

    WorkflowStatus getWorkflowStatusFromApplicationId(String appId);

    ApplicationId submit(WorkflowConfiguration configuration);

}
