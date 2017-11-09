package com.latticeengines.workflow.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public interface WorkflowJobEntityMgr extends BaseEntityMgr<WorkflowJob> {

    WorkflowJob findByApplicationId(String applicationId);

    WorkflowJob findByWorkflowId(long workflowId);

    WorkflowJob findByWorkflowIdWithFilter(long workflowId);

    List<WorkflowJob> findByTenant(Tenant tenant);

    WorkflowJob updateStatusFromYarn(WorkflowJob workflowJob,
            com.latticeengines.domain.exposed.dataplatform.JobStatus yarnJobStatus);

    void updateWorkflowJob(WorkflowJob workflowJob);

    void registerWorkflowId(WorkflowJob workflowJob);
}
