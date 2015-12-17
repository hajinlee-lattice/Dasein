package com.latticeengines.propdata.collection.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.propdata.collection.dao.ArchiveProgressDao;
import com.latticeengines.propdata.collection.source.Source;

@Component("archiveProgressDao")
public class ArchiveProgressDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<ArchiveProgress>
        implements ArchiveProgressDao {

    @Override
    protected Class<ArchiveProgress> getEntityClass() {
        return ArchiveProgress.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ArchiveProgress findByRootOperationUid(String uid) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where RootOperationUID = :rootUid", getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("rootUid", uid.toUpperCase());
        List<ArchiveProgress> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ArchiveProgress> findFailedProgresses(Source source) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = %s and Status = 'FAILED' order by LatestStatusUpdate asc",
                sourceName, getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        return (List<ArchiveProgress>) query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ArchiveProgress> findAllOfSource(Source source) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = %s order by LatestStatusUpdate asc",
                sourceName, getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        return (List<ArchiveProgress>) query.list();
    }

}
