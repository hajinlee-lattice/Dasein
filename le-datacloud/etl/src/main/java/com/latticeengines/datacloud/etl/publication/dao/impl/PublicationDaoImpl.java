package com.latticeengines.datacloud.etl.publication.dao.impl;

import java.util.List;

import org.hibernate.query.Query;
import org.hibernate.Session;

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
    public List<Publication> findAllForSource(String sourceName) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = :sourceName",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("sourceName", sourceName);
        return query.list();
    }

}
