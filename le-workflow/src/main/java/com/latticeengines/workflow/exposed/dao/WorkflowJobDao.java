package com.latticeengines.workflow.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.workflow.exposed.build.Workflow;

public interface WorkflowJobDao extends BaseDao<WorkflowJob> {

    WorkflowJob findByApplicationId(String applicationId);

    WorkflowJob findByWorkflowId(long workflowId);

    List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds);

    List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds, List<String> types);

    List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds, List<String> types, Long parentJobId);

    List<WorkflowJob> findByTenant(Tenant tenant);

    List<WorkflowJob> findByTenant(Tenant tenant, List<String> types);

    List<WorkflowJob> findByTenantAndWorkflowIds(Tenant tenant, List<Long> workflowIds);

    void updateStatus(WorkflowJob workflowJob);

    void updateStatusAndStartTime(WorkflowJob workflowJob);

    void updateParentJobId(WorkflowJob workflowJob);

    void registerWorkflowId(WorkflowJob workflowJob);

    void updateReport(WorkflowJob workflowJob);

    void updateOutput(WorkflowJob workflowJob);

    void updateErrorDetails(WorkflowJob workflowJob);

}
