package com.latticeengines.workflow.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;

public interface WorkflowJobUpdateDao extends BaseDao<WorkflowJobUpdate> {
    WorkflowJobUpdate findByWorkflowPid(Long workflowPid);

    List<WorkflowJobUpdate> findByWorkflowPids(List<Long> workflowPids);

    List<WorkflowJobUpdate> findByLastUpdateTime(Long lastUpdateTime);

    void updateLastUpdateTime(WorkflowJobUpdate workflowJobUpdate);
}
