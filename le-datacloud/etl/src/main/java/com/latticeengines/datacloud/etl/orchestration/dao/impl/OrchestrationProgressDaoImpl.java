package com.latticeengines.datacloud.etl.orchestration.dao.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.orchestration.dao.OrchestrationProgressDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

@Component("orchestrationProgressDao")
public class OrchestrationProgressDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<OrchestrationProgress>
        implements OrchestrationProgressDao {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(OrchestrationProgressDaoImpl.class);

    @Override
    protected Class<OrchestrationProgress> getEntityClass() {
        return OrchestrationProgress.class;
    }

    /*
     * Map Fields: ColumnName -> Value Requires to pass in column name in table,
     * not variable name in OrchestrationProgress entity class, also foreign key
     * not working using column name
     */

    @SuppressWarnings("unchecked")
    @Override

    public List<OrchestrationProgress> findProgressesByField(Map<String, Object> fields, List<String> orderFields) {
        Session session = getSessionFactory().getCurrentSession();
        Class<OrchestrationProgress> entityClz = getEntityClass();
        String orderStr = "";
        if (CollectionUtils.isNotEmpty(orderFields)) {
            orderStr = "order by " + String.join(", ", orderFields);
        }
        String queryStr = String.format(
                "from %s p where p.orchestration.name = :name and p.status = :ProgressStatus %s",
                entityClz.getSimpleName(), orderStr);
        Query<OrchestrationProgress> query = session.createQuery(queryStr);

        for (String column : fields.keySet()) {
                query.setParameter(column, fields.get(column));
        }

        return query.list();
    }


    @Override
    public boolean isDuplicateVersion(String orchName, String version) {
        Session session = getSessionFactory().getCurrentSession();
        Class<OrchestrationProgress> entityClz = getEntityClass();
        String queryStr = String.format("from %s p where p.version = :version and p.orchestration.name = :name",
                entityClz.getSimpleName());
        Query<OrchestrationProgress> query = session.createQuery(queryStr, OrchestrationProgress.class);
        query.setParameter("version", version);
        query.setParameter("name", orchName);
        return !CollectionUtils.isEmpty(query.list());
    }

    @Override
    public List<OrchestrationProgress> findProgressesToCheckStatus() {
        Session session = getSessionFactory().getCurrentSession();
        Class<OrchestrationProgress> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s p where p.status = :newStatus or p.status = :processingStatus or (p.status = :failedStatus and p.retries < p.orchestration.maxRetries)",
                entityClz.getSimpleName());
        Query<OrchestrationProgress> query = session.createQuery(queryStr, OrchestrationProgress.class);
        query.setParameter("newStatus", ProgressStatus.NEW);
        query.setParameter("processingStatus", ProgressStatus.PROCESSING);
        query.setParameter("failedStatus", ProgressStatus.FAILED);
        return query.list();
    }

}
