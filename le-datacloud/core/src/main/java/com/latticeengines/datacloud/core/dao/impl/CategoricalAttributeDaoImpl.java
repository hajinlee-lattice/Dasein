package com.latticeengines.datacloud.core.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;

import com.latticeengines.datacloud.core.dao.CategoricalAttributeDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;

public class CategoricalAttributeDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<CategoricalAttribute>
        implements CategoricalAttributeDao {

    @Override
    protected Class<CategoricalAttribute> getEntityClass() {
        return CategoricalAttribute.class;
    }

    @Override
    public CategoricalAttribute findByNameValue(String attrName, String attrValue) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where attrName = :attrName and attrValue = :attrValue",
                getEntityClass().getSimpleName());
        Query<CategoricalAttribute> query = session.createQuery(queryStr, CategoricalAttribute.class);
        query.setParameter("attrName", attrName);
        query.setParameter("attrValue", attrValue);
        List<?> results = query.list();
        if (results == null || results.isEmpty()) {
            return null;
        } else {
            return (CategoricalAttribute) query.list().get(0);
        }
    }

}
