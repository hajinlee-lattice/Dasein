package com.latticeengines.datacloud.core.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;

import com.latticeengines.datacloud.core.dao.CategoricalDimensionDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;

public class CategoricalDimensionDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<CategoricalDimension>
        implements CategoricalDimensionDao {

    @Override
    protected Class<CategoricalDimension> getEntityClass() {
        return CategoricalDimension.class;
    }

    @Override
    public CategoricalDimension findBySourceDimension(String source, String dimension) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where source = :source and dimension = :dimension",
                getEntityClass().getSimpleName());
        Query<CategoricalDimension> query = session.createQuery(queryStr, CategoricalDimension.class);
        query.setParameter("source", source);
        query.setParameter("dimension", dimension);
        List<?> results = query.list();
        if (results == null || results.isEmpty()) {
            return null;
        } else {
            return (CategoricalDimension) query.list().get(0);
        }
    }
}
