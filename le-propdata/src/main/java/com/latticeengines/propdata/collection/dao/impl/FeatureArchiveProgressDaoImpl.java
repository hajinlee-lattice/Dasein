package com.latticeengines.propdata.collection.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.collection.FeatureArchiveProgress;
import com.latticeengines.propdata.collection.dao.FeatureArchiveProgressDao;

@Component("featureArchiveProgressDao")
public class FeatureArchiveProgressDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<FeatureArchiveProgress>
        implements FeatureArchiveProgressDao {

    @Override
    protected Class<FeatureArchiveProgress> getEntityClass() {
        return FeatureArchiveProgress.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public FeatureArchiveProgress findByRootOperationUid(String uid) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where RootOperationUID = :rootUid", getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("rootUid", uid.toUpperCase());
        List<FeatureArchiveProgress> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<FeatureArchiveProgress> findFailedProgresses() {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where Status = 'FAILED' order by LatestStatusUpdate asc",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        return (List<FeatureArchiveProgress>) query.list();
    }

}
