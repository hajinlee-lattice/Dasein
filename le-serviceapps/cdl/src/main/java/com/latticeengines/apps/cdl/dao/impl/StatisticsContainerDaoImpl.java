package com.latticeengines.apps.cdl.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.StatisticsContainerDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;

@Component("statisticsContainerDao")
public class StatisticsContainerDaoImpl extends BaseDaoImpl<StatisticsContainer> implements StatisticsContainerDao {
    @Override
    protected Class<StatisticsContainer> getEntityClass() {
        return StatisticsContainer.class;
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    public StatisticsContainer findInSegment(String segmentName, DataCollection.Version version) {
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "from %s as stat";
        queryPattern += " where stat.segment.name = :segmentName";
        if (version != null) {
            queryPattern += " and stat.version = :version";
        }
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query<StatisticsContainer> query = session.createQuery(queryStr);
        query.setString("segmentName", segmentName);
        if (version != null) {
            query.setParameter("version", version);
        }
        List<?> list = query.list();
        return (StatisticsContainer) list.stream().findFirst().orElse(null);
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    public StatisticsContainer findInMasterSegment(String collectionName, DataCollection.Version version) {
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "from %s as stat";
        queryPattern += " where stat.segment.dataCollection.name = :collectionName";
        queryPattern += " and stat.segment.isMasterSegment = :isMaster";
        if (version != null) {
            queryPattern += " and stat.version = :version";
        }
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query<StatisticsContainer> query = session.createQuery(queryStr);
        query.setString("collectionName", collectionName);
        query.setBoolean("isMaster", true);
        if (version != null) {
            query.setParameter("version", version);
        }
        List<?> list = query.list();
        return (StatisticsContainer) list.stream().findFirst().orElse(null);
    }
}
