package com.latticeengines.workflow.exposed.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowAppContext;
import com.latticeengines.workflow.exposed.dao.WorkflowAppContextDao;
import com.latticeengines.workflow.exposed.entitymgr.WorkflowAppContextEntityMgr;

@Component("workflowAppContextEntityMgr")
public class WorkflowAppContextEntityMgrImpl extends BaseEntityMgrImpl<WorkflowAppContext> implements WorkflowAppContextEntityMgr {

    @Autowired
    private WorkflowAppContextDao workflowAppContextDao;

    public WorkflowAppContextEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<WorkflowAppContext> getDao() {
        return workflowAppContextDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<WorkflowAppContext> findWorkflowIdsByTenant(Tenant tenant) {
        return workflowAppContextDao.findWorkflowIdsByTenant(tenant);
    }

}
