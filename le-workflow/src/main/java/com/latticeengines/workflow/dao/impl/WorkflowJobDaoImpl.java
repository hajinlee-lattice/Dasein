package com.latticeengines.workflow.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
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
    public WorkflowJob findByWorkflowPid(long workflowPid) {
        return findByKey(WorkflowJob.class, workflowPid);
    }

    @Override
    public WorkflowJob findByApplicationId(String applicationId) {
        return findByField("applicationId", applicationId);
    }

    @Override
    public WorkflowJob deleteByApplicationId(String applicationId) {
        WorkflowJob workflowJob = findByApplicationId(applicationId);
        if (workflowJob != null) {
            deleteByPid(workflowJob.getPid(), true);
        }
        return workflowJob;
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
    public List<WorkflowJob> findByWorkflowPids(List<Long> workflowPids) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format("from %s workflowjob where workflowjob.pid in :pids",
                entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameterList("pids", workflowPids);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds, List<String> types) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s workflowjob where workflowjob.workflowId in :workflowIds and workflowjob.type in :types",
                entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameterList("workflowIds", workflowIds);
        query.setParameterList("types", types);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds, Long parentJobId) {
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
    public List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds, List<String> types, Long parentJobId) {
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
    public List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds, List<String> types, List<String> statuses, Long parentJobId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();

        StringBuffer queryStr = new StringBuffer(String.format("from %s workflowjob", entityClz.getSimpleName()));

        List<String> queryCriteriaClauses = new ArrayList<>();
        Map<String, Object> queryParams = new HashMap<>();

        if (CollectionUtils.isNotEmpty(workflowIds)) {
            queryCriteriaClauses.add(" workflowjob.workflowId in :workflowIds ");
            queryParams.put("workflowIds", workflowIds);
        }
        if (CollectionUtils.isNotEmpty(types)) {
            queryCriteriaClauses.add(" workflowjob.type in :types ");
            queryParams.put("types", types);
        }
        if (CollectionUtils.isNotEmpty(statuses)) {
            queryCriteriaClauses.add(" workflowjob.status in :statuses ");
            queryParams.put("statuses", statuses);
        }
        if (parentJobId != null) {
            queryCriteriaClauses.add(" workflowjob.parentJobId=:parentJobId ");
            queryParams.put("parentJobId", parentJobId);
        }

        if (!queryCriteriaClauses.isEmpty()) {
            queryStr.append(" WHERE ").append(String.join(" AND ", queryCriteriaClauses));
        }

        Query<WorkflowJob> query = session.createQuery(queryStr.toString());
        queryParams.keySet().stream().forEach(param -> {
            Object value = queryParams.get(param);
            if(value instanceof List) {
                query.setParameterList(param, (List)value);
            } else {
                query.setParameter(param, value);
            }
        });

        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByWorkflowPids(List<Long> workflowPids, List<String> types) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s workflowjob where workflowjob.pid in :pids and workflowjob.type in :types",
                entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameterList("pids", workflowPids);
        query.setParameterList("types", types);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByWorkflowPids(List<Long> workflowPids, Long parentJobId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s workflowjob where workflowjob.pid in :pids and workflowjob.parentJobId=:parentJobId",
                entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameterList("pids", workflowPids);
        query.setParameter("parentJobId", parentJobId);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowJob> findByWorkflowPids(List<Long> workflowPids, List<String> types, Long parentJobId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format("from %s workflowjob where workflowjob.pid in :pids and " +
                        "workflowjob.type in :types and workflowjob.parentJobId=:parentJobId",
                entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameterList("pids", workflowPids);
        query.setParameterList("types", types);
        query.setParameter("parentJobId", parentJobId);
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
    public List<WorkflowJob> findByTypes(List<String> types, Long parentJobId) {
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
    public List<WorkflowJob> findByTenantAndWorkflowPids(Tenant tenant, List<Long> workflowPids) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format("from %s workflowjob where workflowjob.tenant.pid=:tenantPid and " +
                "workflowjob.pid in :pids", entityClz.getSimpleName());
        Query<WorkflowJob> query = session.createQuery(queryStr);
        query.setParameter("tenantPid", tenant.getPid());
        query.setParameter("pids", workflowPids);
        return query.list();
    }

    @Override
    public List<WorkflowJob> findByClusterIDAndTypesAndStatuses(String clusterId, List<String> workflowTypes,
            List<String> statuses) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        StringBuilder sb = new StringBuilder().append(String.format("from %s workflowjob", entityClz.getSimpleName()));
        boolean hasClause = false;
        // build query when condition is provided
        if (StringUtils.isNotBlank(clusterId)) {
            appendWhereClause(sb, hasClause, " workflowjob.emrClusterId=:clusterId");
            hasClause = true;
        }
        if (CollectionUtils.isNotEmpty(workflowTypes)) {
            appendWhereClause(sb, hasClause, " workflowjob.type in :types");
            hasClause = true;
        }
        if (CollectionUtils.isNotEmpty(statuses)) {
            appendWhereClause(sb, hasClause, " workflowjob.status in :statuses");
            hasClause = true;
        }

        Query<WorkflowJob> query = session.createQuery(sb.toString(), entityClz);
        // set parameter values
        if (StringUtils.isNotBlank(clusterId)) {
            query.setParameter("clusterId", clusterId);
        }
        if (CollectionUtils.isNotEmpty(workflowTypes)) {
            query.setParameter("types", workflowTypes);
        }
        if (CollectionUtils.isNotEmpty(statuses)) {
            query.setParameter("statuses", statuses);
        }

        return query.list();
    }

    private void appendWhereClause(StringBuilder sb, boolean hasClause, String clause) {
        if (hasClause) {
            sb.append(" and");
        } else {
            sb.append(" where");
        }
        sb.append(clause);
    }

    @Override
    public void updateStatusAndStartTime(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.status=:status, " +
                        "workflowjob.startTimeInMillis=:startTimeInMillis where workflowjob.pid=:pid",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("status", workflowJob.getStatus());
        query.setParameter("startTimeInMillis", workflowJob.getStartTimeInMillis());
        query.setParameter("pid", workflowJob.getPid());
        query.executeUpdate();
    }

    @Override
    public void updateStatus(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.status=:status where workflowjob.pid=:pid",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("status", workflowJob.getStatus());
        query.setParameter("pid", workflowJob.getPid());
        query.executeUpdate();
    }

    @Override
    public void updateParentJobId(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.parentJobId=:parentJobId where workflowjob.pid=:pid",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("parentJobId", workflowJob.getParentJobId());
        query.setParameter("pid", workflowJob.getPid());
        query.executeUpdate();
    }

    @Override
    public void registerWorkflowId(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.workflowId=:workflowId where workflowjob.pid=:pid",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("workflowId", workflowJob.getWorkflowId());
        query.setParameter("pid", workflowJob.getPid());
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

    @Override
    public void updateApplicationId(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.applicationId=:applicationId where workflowjob.pid=:pid",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("applicationId", workflowJob.getApplicationId());
        query.setParameter("pid", workflowJob.getPid());
        query.executeUpdate();
    }

    @Override
    public void updateApplicationIdAndEmrClusterId(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format("update %s workflowjob " //
                        + "set workflowjob.applicationId=:applicationId, "
                        + "workflowjob.emrClusterId=:emrClusterId "
                        + "where workflowjob.pid=:pid",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("applicationId", workflowJob.getApplicationId());
        query.setParameter("emrClusterId", workflowJob.getEmrClusterId());
        query.setParameter("pid", workflowJob.getPid());
        query.executeUpdate();
    }

    @Override
    public void updateErrorCategory(WorkflowJob workflowJob) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s workflowjob set workflowjob.errorCategory=:errorCategory where workflowjob.pid=:pid",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("errorCategory", workflowJob.getErrorCategory());
        query.setParameter("pid", workflowJob.getPid());
        query.executeUpdate();
    }

    @Override
    public void deleteByTenantPid(Long tenantPid) {
        Session session = getSessionFactory().getCurrentSession();
        Class<WorkflowJob> entityClz = getEntityClass();
        String queryStr = String.format(
                "delete from  %s workflowjob where workflowjob.tenantId=:tenantId", entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("tenantId", tenantPid);
        query.executeUpdate();
    }
}
