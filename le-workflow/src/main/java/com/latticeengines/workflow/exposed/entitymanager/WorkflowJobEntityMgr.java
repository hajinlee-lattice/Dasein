package com.latticeengines.workflow.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public interface WorkflowJobEntityMgr extends BaseEntityMgr<WorkflowJob> {

    WorkflowJob findByApplicationId(String applicationId);

    WorkflowJob findByApplicationIdWithFilter(String applicationId);

    WorkflowJob findByWorkflowId(long workflowId);

    WorkflowJob findByWorkflowIdWithFilter(long workflowId);

    List<WorkflowJob> findAllWithFilter();

    List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds);

    List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds, List<String> types);

    List<WorkflowJob> findByWorkflowIdsOrTypesOrParentJobId(List<Long> workflowIds, List<String> types, Long parentJobId);

    List<WorkflowJob> findByWorkflowIdsOrTypesOrParentJobIdWithFilter(List<Long> workflowIds, List<String> types, Long parentJobId);

    List<WorkflowJob> findByTenant(Tenant tenant);

    List<WorkflowJob> findByTenant(Tenant tenant, List<String> types);

    List<WorkflowJob> findByTenantAndWorkflowIds(Tenant tenant, List<Long> workflowIds);

    WorkflowJob updateStatusFromYarn(WorkflowJob workflowJob,
            com.latticeengines.domain.exposed.dataplatform.JobStatus yarnJobStatus);

    void updateWorkflowJob(WorkflowJob workflowJob);

    void updateWorkflowJobStatus(WorkflowJob workflowJob);

    void updateParentJobId(WorkflowJob workflowJob);

    void registerWorkflowId(WorkflowJob workflowJob);

    void updateReport(WorkflowJob workflowJob);

    void updateOutput(WorkflowJob workflowJob);

    void updateErrorDetails(WorkflowJob job);
}
