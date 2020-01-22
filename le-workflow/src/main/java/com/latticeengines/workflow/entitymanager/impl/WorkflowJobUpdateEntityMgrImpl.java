package com.latticeengines.workflow.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.workflow.exposed.dao.WorkflowJobUpdateDao;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;

@Component("workflowJobUpdateEntityMgr")
public class WorkflowJobUpdateEntityMgrImpl extends BaseEntityMgrImpl<WorkflowJobUpdate>
        implements WorkflowJobUpdateEntityMgr {

    @Inject
    WorkflowJobUpdateDao workflowJobUpdateDao;

    @Override
    public BaseDao<WorkflowJobUpdate> getDao() {
        return workflowJobUpdateDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void create(WorkflowJobUpdate workflowJobUpdate) {
        super.create(workflowJobUpdate);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public WorkflowJobUpdate findByWorkflowPid(Long workflowPid) {
        return workflowJobUpdateDao.findByWorkflowPid(workflowPid);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJobUpdate> findByLastUpdateTime(Long lastUpdateTime) {
        return workflowJobUpdateDao.findByLastUpdateTime(lastUpdateTime);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateLastUpdateTime(WorkflowJobUpdate workflowJobUpdate) {
        workflowJobUpdateDao.updateLastUpdateTime(workflowJobUpdate);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public WorkflowJobUpdate deleteByWorkflowPid(Long workflowPid) {
        return workflowJobUpdateDao.deleteByWorkflowPid(workflowPid);
    }
}
