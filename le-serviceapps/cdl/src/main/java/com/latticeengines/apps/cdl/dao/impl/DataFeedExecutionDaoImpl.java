package com.latticeengines.apps.cdl.dao.impl;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataFeedExecutionDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;

@Component("datafeedExecutionDao")
public class DataFeedExecutionDaoImpl extends BaseDaoImpl<DataFeedExecution> implements DataFeedExecutionDao {

    @Override
    protected Class<DataFeedExecution> getEntityClass() {
        return DataFeedExecution.class;
    }

    @Override
    public DataFeedExecution updateStatus(DataFeedExecution execution) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedExecution> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s execution set execution.status=:status where execution.pid=:pid",
                entityClz.getSimpleName());
        @SuppressWarnings("unchecked")
        Query<DataFeedExecution> query = session.createQuery(queryStr);
        query.setParameter("status", execution.getStatus());
        query.setParameter("pid", execution.getPid());
        query.executeUpdate();
        return execution;
    }

    @Override
    public DataFeedExecution updateRetryCount(DataFeedExecution execution) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedExecution> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s execution set execution.retryCount=:retryCount where execution.pid=:pid",
                entityClz.getSimpleName());
        @SuppressWarnings("unchecked")
        Query<DataFeedExecution> query = session.createQuery(queryStr);
        query.setParameter("retryCount", execution.getRetryCount());
        query.setParameter("pid", execution.getPid());
        query.executeUpdate();
        return execution;
    }
}
