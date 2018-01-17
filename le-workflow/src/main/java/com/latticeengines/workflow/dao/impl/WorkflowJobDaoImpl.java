package com.latticeengines.workflow.dao.impl;

import java.util.List;

import org.hibernate.query.Query;
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
    public List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format("from %s workflowjob where workflowjob.workflowId in :workflowIds",
                entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameterList("workflowIds", workflowIds);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByTypes(List<String> types) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format("from %s workflowjob where workflowjob.type in :types",
                entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameterList("types", types);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByWorkflowIdsAndTypes(List<Long> workflowIds, List<String> types) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s workflowjob where workflowjob.workflowId in :workflowIds and workflowjob.type in :types",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameterList("workflowIds", workflowIds);
        query.setParameterList("types", types);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByWorkflowIdsAndParentJobId(List<Long> workflowIds, Long parentJobId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s workflowjob where workflowjob.workflowId in :workflowIds and workflowjob.parentJobId=:parentJobId",
                entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameterList("workflowIds", workflowIds);
        query.setParameter("parentJobId", parentJobId);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByTypesAndParentJobId(List<String> types, Long parentJobId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s workflowjob where workflowjob.type in :types and workflowjob.parentJobId=:parentJobId",
                entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameterList("types", types);
        query.setParameter("parentJobId", parentJobId);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByWorkflowIdsAndTypesAndParentJobId(List<Long> workflowIds, List<String> types,
                                                                     Long parentJobId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format("from %s workflowjob where workflowjob.workflowId in :workflowIds and " +
                        "workflowjob.type in :types and workflowjob.parentJobId=:parentJobId",
                entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameterList("workflowIds", workflowIds);
        query.setParameterList("types", types);
        query.setParameter("parentJobId", parentJobId);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByTenant(Tenant tenant) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format("from %s workflowjob where workflowjob.tenant.pid=:tenantPid",
                entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameter("tenantPid", tenant.getPid());
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByTenant(Tenant tenant, List<String> types) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format("from %s workflowjob where workflowjob.tenant.pid=:tenantPid and " +
                "workflowjob.type in :types", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("tenantPid", tenant.getPid());
        query.setParameterList("types", types);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByTenantAndWorkflowIds(Tenant tenant, List<Long> workflowIds) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format("from %s workflowjob where workflowjob.tenant.pid=:tenantPid and " +
                "workflowjob.workflowId in :workflowIds", entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameter("tenantPid", tenant.getPid());
        query.setParameter("workflowIds", workflowIds);
        return query.list();
    }

    @Override
    public void updateStatusAndStartTime(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.status=:status, workflowjob.startTimeInMillis=:startTimeInMillis where workflowjob.applicationId=:applicationId",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("status", workflowJob.getStatus());
        query.setParameter("startTimeInMillis", workflowJob.getStartTimeInMillis());
        query.setParameter("applicationId", workflowJob.getApplicationId());
        query.executeUpdate();
    }

    @Override
    public void updateStatus(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.status=:status where workflowjob.applicationId=:applicationId",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("status", workflowJob.getStatus());
        query.setParameter("applicationId", workflowJob.getApplicationId());
        query.executeUpdate();
    }

    @Override
    public void updateParentJobId(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.parentJobId=:parentJobId where workflowjob.workflowId=:workflowId",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("parentJobId", workflowJob.getParentJobId());
        query.setParameter("workflowId", workflowJob.getWorkflowId());
        query.executeUpdate();
    }

    @Override
    public void registerWorkflowId(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.workflowId=:workflowId where workflowjob.applicationId=:applicationId",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("workflowId", workflowJob.getWorkflowId());
        query.setParameter("applicationId", workflowJob.getApplicationId());
        query.executeUpdate();
    }

    @Override
    public void updateReport(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.reportContextString=:reportContextString where workflowjob.pid=:pid",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("reportContextString", workflowJob.getReportContextString());
        query.setParameter("pid", workflowJob.getPid());
        query.executeUpdate();
    }

    @Override
    public void updateOutput(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.outputContextString=:outputContextString where workflowjob.pid=:pid",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("outputContextString", workflowJob.getOutputContextString());
        query.setParameter("pid", workflowJob.getPid());
        query.executeUpdate();
    }

    @Override
    public void updateErrorDetails(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.errorDetailsString=:errorDetailsString where workflowjob.pid=:pid",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("errorDetailsString", workflowJob.getErrorDetailsString());
        query.setParameter("pid", workflowJob.getPid());
        query.executeUpdate();
    }
}
