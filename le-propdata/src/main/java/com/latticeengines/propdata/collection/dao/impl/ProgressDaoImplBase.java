package com.latticeengines.propdata.collection.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.collection.Progress;
import com.latticeengines.propdata.collection.dao.ProgressDao;
import com.latticeengines.propdata.core.source.Source;

public abstract class ProgressDaoImplBase<P extends Progress>
        extends BaseDaoWithAssignedSessionFactoryImpl<P>
        implements ProgressDao<P> {

    @SuppressWarnings("unchecked")
    @Override
    public P findByRootOperationUid(String uid) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where RootOperationUID = :rootUid", getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("rootUid", uid.toUpperCase());
        List<P> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<P> findFailedProgresses(Source source) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = :sourceName and Status = 'FAILED' order by LatestStatusUpdate asc",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("sourceName", sourceName);
        return (List<P>) query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<P> findUnfinishedProgresses(Source source) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = :sourceName " +
                "and Status != 'FINISHED' and Status != 'FAILED' " +
                "order by CreateTime asc",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("sourceName", sourceName);
        return (List<P>) query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<P> findAllOfSource(Source source) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = :sourceName",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("sourceName", sourceName);
        return (List<P>) query.list();
    }

}
