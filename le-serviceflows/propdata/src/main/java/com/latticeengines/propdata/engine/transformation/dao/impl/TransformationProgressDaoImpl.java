package com.latticeengines.propdata.engine.transformation.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.dao.TransformationProgressDao;

@Component("transformationProgressDao")
public class TransformationProgressDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<TransformationProgress>
        implements TransformationProgressDao {

    @Override
    protected Class<TransformationProgress> getEntityClass() {
        return TransformationProgress.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TransformationProgress findByRootOperationUid(String uid) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where RootOperationUID = :rootUid", getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("rootUid", uid.toUpperCase());
        List<TransformationProgress> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TransformationProgress> findFailedProgresses(Source source) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where SourceName = :sourceName and Status = 'FAILED' order by LatestStatusUpdate asc",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("sourceName", sourceName);
        return (List<TransformationProgress>) query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TransformationProgress> findUnfinishedProgresses(Source source) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = :sourceName "
                + "and Status != 'FINISHED' and Status != 'FAILED' " + "order by CreateTime asc",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("sourceName", sourceName);
        return (List<TransformationProgress>) query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TransformationProgress> findAllOfSource(Source source) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = :sourceName", getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("sourceName", sourceName);
        return (List<TransformationProgress>) query.list();
    }

}
