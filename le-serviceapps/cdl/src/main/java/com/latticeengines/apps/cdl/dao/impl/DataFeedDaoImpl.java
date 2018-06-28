package com.latticeengines.apps.cdl.dao.impl;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataFeedDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;

@Component("datafeedDao")
public class DataFeedDaoImpl extends BaseDaoImpl<DataFeed> implements DataFeedDao {

    @Override
    protected Class<DataFeed> getEntityClass() {
        return DataFeed.class;
    }

    @Override
    public DataFeed updateStatus(DataFeed dataFeed) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeed> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s feed set feed.status=:status where feed.pid=:pid",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("status", dataFeed.getStatus());
        query.setParameter("pid", dataFeed.getPid());
        query.executeUpdate();
        return dataFeed;
    }
}
