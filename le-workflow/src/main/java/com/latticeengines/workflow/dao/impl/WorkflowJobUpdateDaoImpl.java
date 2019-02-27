package com.latticeengines.workflow.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.workflow.exposed.dao.WorkflowJobUpdateDao;


@Component("workflowJobUpdateDao")
public class WorkflowJobUpdateDaoImpl extends BaseDaoImpl<WorkflowJobUpdate> implements WorkflowJobUpdateDao {
    @Override
    protected Class<WorkflowJobUpdate> getEntityClass() {
        return WorkflowJobUpdate.class;
    }

    @Override
    public WorkflowJobUpdate findByWorkflowPid(Long workflowPid) {
        return findByField("workflowPid", workflowPid);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJobUpdate> findByWorkflowPids(List<Long> workflowPids) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJobUpdate> entityClz = getEntityClass();
        String queryStr = String.format("from %s wfupdate where wfupdate.workflowPid in :workflowPids",
                entityClz.getSimpleName());
        Query<WorkflowJobUpdate> query = session.createQuery(queryStr);
        query.setParameterList("workflowPids", workflowPids);
        return query.list();
    }

    @Override
    public List<WorkflowJobUpdate> findByLastUpdateTime(Long lastUpdateTime) {
        return findAllByField("lastUpdateTime", lastUpdateTime);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void updateLastUpdateTime(WorkflowJobUpdate workflowJobUpdate) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJobUpdate> entityClz = getEntityClass();
        String queryStr = String.format("update %s wfupdate set wfupdate.lastUpdateTime=:lastUpdateTime " +
                        "where wfupdate.pid=:pid", entityClz.getSimpleName());
        Query<WorkflowJobUpdate> query = session.createQuery(queryStr);
        query.setParameter("lastUpdateTime", workflowJobUpdate.getLastUpdateTime());
        query.setParameter("pid", workflowJobUpdate.getPid());
        query.executeUpdate();
    }

    @Override
    public WorkflowJobUpdate deleteByWorkflowPid(Long workflowJobPid) {
        WorkflowJobUpdate jobUpdate = findByWorkflowPid(workflowJobPid);
        if (jobUpdate != null) {
            deleteByPid(jobUpdate.getPid(), true);
        }
        return jobUpdate;
    }
}
