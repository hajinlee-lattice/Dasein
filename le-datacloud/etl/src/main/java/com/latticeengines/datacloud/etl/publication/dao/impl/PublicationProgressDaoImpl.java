package com.latticeengines.datacloud.etl.publication.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.datacloud.etl.publication.dao.PublicationProgressDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;

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

    @SuppressWarnings("unchecked")
    @Override
    public List<PublicationProgress> getStatusForLatestVersion(Publication publication, String version) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s p where p.publication.pid = :pid and sourceVersion = :sourceVersion order by createTime desc limit 1",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("pid", publication.getPid());
        query.setParameter("sourceVersion", version);
        return query.list();
    }
}
