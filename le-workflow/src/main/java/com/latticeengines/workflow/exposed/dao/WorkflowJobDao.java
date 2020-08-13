package com.latticeengines.workflow.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public interface WorkflowJobDao extends BaseDao<WorkflowJob> {

    WorkflowJob findByWorkflowPid(long workflowPid);

    WorkflowJob findByApplicationId(String applicationId);

    WorkflowJob deleteByApplicationId(String applicationId);

    WorkflowJob findByWorkflowId(long workflowId);

    List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds);

    List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds, List<String> types);

    List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds, Long parentJobId);

    List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds, List<String> types, Long parentJobId);

    List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds, List<String> types, List<String> statuses, Long parentJobId);

    List<WorkflowJob> findByWorkflowPids(List<Long> workflowPids);

    List<WorkflowJob> findByWorkflowPids(List<Long> workflowPids, List<String> types);

    List<WorkflowJob> findByWorkflowPids(List<Long> workflowPids, Long parentJobId);

    List<WorkflowJob> findByWorkflowPids(List<Long> workflowPids, List<String> types, Long parentJobId);

    List<WorkflowJob> findByTypes(List<String> types);

    List<WorkflowJob> findByTypes(List<String> types, Long parentJobId);

    List<WorkflowJob> findByTenantAndWorkflowPids(Tenant tenant, List<Long> workflowPids);

    /**
     * Retrieve all jobs with given cluster ID, one of the given types and one of
     * the given statuses.
     *
     * @param clusterId
     *            target cluster ID
     * @param tenantPid
     *            target tenant's primary ID
     * @param workflowTypes
     *            target list of workflow types
     * @param statuses
     *            target list of workflow statuses
     * @param earliestStartTime
     *            epoch timestamp that PA should started after
     * @return non-null list of all jobs satisfying all criteria
     */
    List<WorkflowJob> findByClusterIDAndTypesAndStatuses(String clusterId, Long tenantPid, List<String> workflowTypes,
            List<String> statuses, Long earliestStartTime);

    void updateStatus(WorkflowJob workflowJob);

    void updateStatusAndStartTime(WorkflowJob workflowJob);

    void updateParentJobId(WorkflowJob workflowJob);

    void registerWorkflowId(WorkflowJob workflowJob);

    void updateReport(WorkflowJob workflowJob);

    void updateOutput(WorkflowJob workflowJob);

    void updateErrorDetails(WorkflowJob workflowJob);

    void updateApplicationId(WorkflowJob workflowJob);

    void updateApplicationIdAndEmrClusterId(WorkflowJob workflowJob);

    void updateErrorCategory(WorkflowJob workflowJob);

    void deleteByTenantPid(Long tenantPid);

    void updateInput(WorkflowJob workflowJob);

    List<WorkflowJob> findAll(int workflowJobQuotaLimit);
}
