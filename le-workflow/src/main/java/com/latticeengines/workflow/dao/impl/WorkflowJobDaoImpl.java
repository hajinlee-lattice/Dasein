package com.latticeengines.workflow.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.workflow.exposed.dao.WorkflowJobDao;

@Component("workflowJobDao")
public class WorkflowJobDaoImpl extends BaseDaoImpl<WorkflowJob> implements WorkflowJobDao {

    @Override
    protected Class<WorkflowJob> getEntityClass() {
        return WorkflowJob.class;
    }

    @Override
    public WorkflowJob findByApplicationId(String applicationId) {
        return findByField("applicationId", applicationId);
    }

    @Override
    public WorkflowJob findByWorkflowId(long workflowId) {
        return findByField("workflowId", workflowId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByTenant(Tenant tenant) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format("from %s workflowjob where workflowjob.tenant.pid=:tenantPid",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("tenantPid", tenant.getPid());
        return query.list();
    }
}
