package com.latticeengines.datacloud.etl.publication.dao.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.etl.publication.dao.PublicationProgressDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;

public class PublicationProgressDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<PublicationProgress>
        implements PublicationProgressDao {

    @Override
    protected Class<PublicationProgress> getEntityClass() {
        return PublicationProgress.class;
    }

    @Override
    public List<PublicationProgress> findAllForPublication(@NotNull Long publicationId) {
        Preconditions.checkNotNull(publicationId);

        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where FK_Publication = :publicationId",
                getEntityClass().getSimpleName());
        Query<PublicationProgress> query = session.createQuery(queryStr, PublicationProgress.class);
        query.setParameter("publicationId", publicationId);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<PublicationProgress> getStatusForLatestVersion(@NotNull Publication publication,
            @NotNull String version) {
        Preconditions.checkNotNull(publication);
        Preconditions.checkNotNull(publication.getPid());
        Preconditions.checkNotNull(version);

        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s p where p.publication.pid = :pid and sourceVersion = :sourceVersion order by pid desc",
                getEntityClass().getSimpleName());
        Query<PublicationProgress> query = session.createQuery(queryStr);
        query.setParameter("pid", publication.getPid());
        query.setParameter("sourceVersion", version);
        query.setMaxResults(1);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public String getLatestSuccessVersion(@NotNull String publicationName) {
        Preconditions.checkNotNull(publicationName);

        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "select max(sourceVersion) from %s t where t.publication.publicationName = :name and status = :status",
                getEntityClass().getSimpleName());
        Query<String> query = session.createQuery(queryStr);
        query.setParameter("name", publicationName);
        query.setParameter("status", ProgressStatus.FINISHED);
        List<String> list = query.list();
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }
}
