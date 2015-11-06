package com.latticeengines.workflowapi.entitymgr.impl;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.workflow.WorkflowId;
import com.latticeengines.domain.exposed.workflow.YarnAppWorkflowId;
import com.latticeengines.workflowapi.dao.YarnAppWorkflowIdDao;
import com.latticeengines.workflowapi.entitymgr.YarnAppWorkflowIdEntityMgr;

@Component("yarnAppWorkflowIdEntityMgr")
public class YarnAppWorkflowIdEntityMgrImpl extends BaseEntityMgrImpl<YarnAppWorkflowId> implements YarnAppWorkflowIdEntityMgr {

    @Autowired
    private YarnAppWorkflowIdDao yarnAppWorkflowIdDao;

    public YarnAppWorkflowIdEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<YarnAppWorkflowId> getDao() {
        return yarnAppWorkflowIdDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public WorkflowId findWorkflowIdByApplicationId(ApplicationId appId) {
        return yarnAppWorkflowIdDao.findWorkflowIdByApplicationId(appId);
    }

}
