package com.latticeengines.workflow.exposed.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowAppContext;
import com.latticeengines.workflow.exposed.dao.WorkflowAppContextDao;

@Component("workflowAppContextDao")
public class WorkflowAppContextDaoImpl extends BaseDaoImpl<WorkflowAppContext> implements WorkflowAppContextDao {

    public WorkflowAppContextDaoImpl() {
        super();
    }

    @Override
    protected Class<WorkflowAppContext> getEntityClass() {
        return WorkflowAppContext.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowAppContext> findWorkflowIdsByTenant(Tenant tenant) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowAppContext> entityClz = getEntityClass();
        String queryStr = String.format("from %s workflowappcontext where workflowappcontext.tenant.pid=:tenantId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("tenantId", tenant.getPid());
        return query.list();
    }

}