package com.latticeengines.propdata.engine.publication.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.propdata.engine.publication.dao.PublicationProgressDao;

public class PublicationProgressDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<PublicationProgress>
        implements PublicationProgressDao {

    @Override
    protected Class<PublicationProgress> getEntityClass() {
        return PublicationProgress.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<PublicationProgress> findAllForPublication(Long publicationId) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where FK_Publication = :publicationId",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("publicationId", publicationId);
        return query.list();
    }
}
