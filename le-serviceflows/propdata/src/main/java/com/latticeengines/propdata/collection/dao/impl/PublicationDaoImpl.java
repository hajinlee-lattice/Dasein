package com.latticeengines.propdata.collection.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.propdata.collection.dao.PublicationDao;

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
