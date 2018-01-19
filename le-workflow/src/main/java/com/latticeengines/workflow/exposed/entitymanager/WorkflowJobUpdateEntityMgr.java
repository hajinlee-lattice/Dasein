package com.latticeengines.workflow.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;

public interface WorkflowJobUpdateEntityMgr extends BaseEntityMgr<WorkflowJobUpdate> {
    WorkflowJobUpdate findByWorkflowPid(Long workflowPid);

    List<WorkflowJobUpdate> findByLastUpdateTime(Long lastUpdateTime);

    void updateLastUpdateTime(WorkflowJobUpdate workflowJobUpdate);
}
