package com.latticeengines.workflow.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public interface WorkflowJobEntityMgr extends BaseEntityMgr<WorkflowJob> {

    WorkflowJob findByWorkflowPid(long workflowPid);

    WorkflowJob findByApplicationId(String applicationId);

    WorkflowJob findByWorkflowId(long workflowId);

    List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds);

    List<WorkflowJob> findByWorkflowIdsAndTypes(List<Long> workflowIds, List<String> types);

    List<WorkflowJob> findByWorkflowIdsOrTypesOrParentJobId(List<Long> workflowIds, List<String> types,
                                                            Long parentJobId);

    List<WorkflowJob> findByWorkflowIdsOrTypesOrParentJobId(List<Long> workflowIds, List<String> types,
            List<String> statuses, Long parentJobId);

    List<WorkflowJob> findByWorkflowPids(List<Long> workflowPids);

    List<WorkflowJob> findByWorkflowPidsAndTypes(List<Long> workflowIds, List<String> types);

    List<WorkflowJob> findByWorkflowPidsOrTypesOrParentJobId(List<Long> workflowPids, List<String> types,
                                                             Long parentJobId);

    List<WorkflowJob> findByTenantAndWorkflowPids(Tenant tenant, List<Long> workflowPids);

    WorkflowJob updateStatusFromYarn(WorkflowJob workflowJob,
            com.latticeengines.domain.exposed.dataplatform.JobStatus yarnJobStatus);

    void updateWorkflowJob(WorkflowJob workflowJob);

    void updateWorkflowJobStatus(WorkflowJob workflowJob);

    void updateParentJobId(WorkflowJob workflowJob);

    void registerWorkflowId(WorkflowJob workflowJob);

    void updateReport(WorkflowJob workflowJob);

    void updateOutput(WorkflowJob workflowJob);

    void updateErrorDetails(WorkflowJob workflowJob);

    void updateApplicationId(WorkflowJob workflowJob);

    void updateApplicationIdAndEmrClusterId(WorkflowJob workflowJob);

    WorkflowJob deleteByApplicationId(String applicationId);

    void deleteByTenantPid(Long tenantPid);

    void updateErrorCategory(WorkflowJob workflowJob);

    void updateInput(WorkflowJob workflowJob);

    /*
     * Tenant filter disabled methods, use these methods carefully. Intended to be
     * used for cross tenant features. E.g., get all running PA jobs.
     *
     * TODO maybe move these method to another entity manager if there are more of
     * these
     */
    List<WorkflowJob> queryByClusterIDAndTypesAndStatuses(String clusterId, List<String> workflowTypes,
            List<String> statuses);

    List<WorkflowJob> findByStatuses(List<String> statuses);

    List<WorkflowJob> findByStatusesAndClusterId(String clusterId, List<String> statuses);
}
