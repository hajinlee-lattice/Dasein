package com.latticeengines.workflowapi.dao;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.workflow.WorkflowId;
import com.latticeengines.domain.exposed.workflow.YarnAppWorkflowId;

public interface YarnAppWorkflowIdDao extends BaseDao<YarnAppWorkflowId> {

    WorkflowId findWorkflowIdByApplicationId(ApplicationId applicationId);

}
