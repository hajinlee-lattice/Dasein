package com.latticeengines.metadata.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.metadata.dao.StatisticsContainerDao;

@Component("statisticsContainerDao")
public class StatisticsContainerDaoImpl extends BaseDaoImpl<StatisticsContainer> implements StatisticsContainerDao {
    @Override
    protected Class<StatisticsContainer> getEntityClass() {
        return StatisticsContainer.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StatisticsContainer findInSegment(String segmentName) {
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "from %s as stat";
        queryPattern += " where stat.segment.name = :segmentName";
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("segmentName", segmentName);
        List<?> list = query.list();
        return (StatisticsContainer) list.stream().findFirst().orElse(null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public StatisticsContainer findInMasterSegment(String collectionName) {
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "from %s as stat";
        queryPattern += " where stat.segment.dataCollection.name = :collectionName";
        queryPattern += " and stat.segment.isMasterSegment = :isMaster";
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("collectionName", collectionName);
        query.setBoolean("isMaster", true);
        List<?> list = query.list();
        return (StatisticsContainer) list.stream().findFirst().orElse(null);
    }
}
