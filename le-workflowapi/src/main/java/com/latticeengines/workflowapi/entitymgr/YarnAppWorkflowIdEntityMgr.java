package com.latticeengines.workflowapi.entitymgr;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.YarnAppWorkflowId;

public interface YarnAppWorkflowIdEntityMgr extends BaseEntityMgr<YarnAppWorkflowId> {

    WorkflowExecutionId findWorkflowIdByApplicationId(ApplicationId appId);
}
