package com.latticeengines.metadata.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.MetadataProperty;
import com.latticeengines.metadata.dao.BaseMetadataPropertyDao;

abstract class BaseMetadataPropertyDaoImpl<T extends MetadataProperty<O>, O> extends BaseDaoImpl<T>
        implements BaseMetadataPropertyDao<T, O> {

    @SuppressWarnings("unchecked")
    @Override
    public List<T> getPropertiesBelongTo(O Owner) {
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "from %s where owner = :owner";
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("owner", Owner);
        return query.list();
    }

}
