package com.latticeengines.datacloud.etl.publication.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.etl.publication.dao.PublicationDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;

public class PublicationDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<Publication>
        implements PublicationDao {

    @Override
    protected Class<Publication> getEntityClass() {
        return Publication.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Publication> findAllForSource(@NotNull String sourceName) {
        Preconditions.checkNotNull(sourceName);
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = :sourceName",
                getEntityClass().getSimpleName());
        Query<Publication> query = session.createQuery(queryStr);
        query.setParameter("sourceName", sourceName);
        return query.list();
    }
}
