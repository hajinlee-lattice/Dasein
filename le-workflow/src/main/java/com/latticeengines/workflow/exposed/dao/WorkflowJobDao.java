package com.latticeengines.workflow.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public interface WorkflowJobDao extends BaseDao<WorkflowJob> {

    WorkflowJob findByApplicationId(String applicationId);

    WorkflowJob findByWorkflowId(long workflowId);

    List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds);

    List<WorkflowJob> findByTenant(Tenant tenant);

    List<WorkflowJob> findByTenantAndWorkflowIds(Tenant tenant, List<Long> workflowIds);

    void updateStatusFromYarn(WorkflowJob workflowJob, JobStatus yarnJobStatus);

    void registerWorkflowId(WorkflowJob workflowJob);

}
