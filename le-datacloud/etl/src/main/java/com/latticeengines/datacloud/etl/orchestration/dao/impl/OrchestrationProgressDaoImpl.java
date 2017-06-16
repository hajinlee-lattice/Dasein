package com.latticeengines.datacloud.etl.orchestration.dao.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.orchestration.dao.OrchestrationProgressDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

@Component("orchestrationProgressDao")
public class OrchestrationProgressDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<OrchestrationProgress>
        implements OrchestrationProgressDao {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(OrchestrationProgressDaoImpl.class);

    @Override
    protected Class<OrchestrationProgress> getEntityClass() {
        return OrchestrationProgress.class;
    }

    /*
     * Map Fields: ColumnName -> Value Requires to pass in column name in table,
     * not variable name in OrchestrationProgress entity class
     */
    @SuppressWarnings("unchecked")
    @Override
    public List<OrchestrationProgress> findProgressesByField(Map<String, Object> fields, List<String> orderFields) {
        Session session = getSessionFactory().getCurrentSession();
        Class<OrchestrationProgress> entityClz = getEntityClass();
        StringBuilder sb = new StringBuilder();
        for (String column : fields.keySet()) {
            sb.append(column + " = :" + column + " and ");
        }
        String orderStr = "";
        if (CollectionUtils.isNotEmpty(orderFields)) {
            orderStr = "order by " + String.join(", ", orderFields);
        }
        String queryStr = String.format("from %s where %s %s", entityClz.getSimpleName(),
                sb.substring(0, sb.length() - 4), orderStr);
        Query query = session.createQuery(queryStr);
        for (String column : fields.keySet()) {
            if (fields.get(column).getClass().isEnum()) {
                query.setParameter(column, fields.get(column).toString());
            } else {
                query.setParameter(column, fields.get(column));
            }
        }
        return query.list();
    }

    @Override
    public boolean isDuplicateVersion(String orchName, String version) {
        Session session = getSessionFactory().getCurrentSession();
        Class<OrchestrationProgress> entityClz = getEntityClass();
        String queryStr = String.format("from %s p where p.version = :version and p.orchestration.name = :name",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("version", version);
        query.setParameter("name", orchName);
        return !CollectionUtils.isEmpty(query.list());
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<OrchestrationProgress> findProgressesToKickoff() {
        Session session = getSessionFactory().getCurrentSession();
        Class<OrchestrationProgress> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s p where p.status = :newStatus or (p.status = :failedStatus and p.retries < p.orchestration.maxRetries)",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("newStatus", ProgressStatus.NEW);
        query.setParameter("failedStatus", ProgressStatus.FAILED);
        return query.list();
    }

}
