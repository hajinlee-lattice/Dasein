package com.latticeengines.metadata.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.query.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.metadata.dao.DataFeedDao;

@Component("datafeedDao")
public class DataFeedDaoImpl extends BaseDaoImpl<DataFeed> implements DataFeedDao {

    @Override
    protected Class<DataFeed> getEntityClass() {
        return DataFeed.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public DataFeed findDefaultFeed(String collectionName) {
        Session session = sessionFactory.getCurrentSession();
        Class<DataFeed> entityClz = getEntityClass();
        String queryPattern = "select df from %s as df";
        queryPattern += " join df.dataCollection as dc";
        queryPattern += " where dc.name = :collectionName";
        String queryStr = String.format(queryPattern, entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("collectionName", collectionName);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (DataFeed) list.get(0);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public List<SimpleDataFeed> findAllSimpleDataFeeds() {
        Session session = sessionFactory.getCurrentSession();
        Class<DataFeed> entityClz = getEntityClass();
        Query query = session.createQuery(String.format("from %s", entityClz.getSimpleName()));

        List<DataFeed> list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            List<SimpleDataFeed> simpleDataFeeds = new ArrayList<>();
            for (DataFeed dataFeed : list) {
                SimpleDataFeed simpleDataFeed = new SimpleDataFeed();
                simpleDataFeed.setTenant(dataFeed.getTenant());
                simpleDataFeed.setStatus(dataFeed.getStatus());

                simpleDataFeeds.add(simpleDataFeed);
            }
            return simpleDataFeeds;
        }
    }
}
