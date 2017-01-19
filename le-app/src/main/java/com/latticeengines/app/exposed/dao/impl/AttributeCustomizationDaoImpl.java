package com.latticeengines.app.exposed.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.dao.AttributeCustomizationDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.AttributeCustomization;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;

@Component("attributeCustomizationDao")
public class AttributeCustomizationDaoImpl extends BaseDaoImpl<AttributeCustomization> implements
        AttributeCustomizationDao {

    @Override
    protected Class<AttributeCustomization> getEntityClass() {
        return AttributeCustomization.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AttributeCustomization find(String attributeName, AttributeUseCase useCase) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where attributeName = :attributeName and useCase = :useCase",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("attributeName", attributeName);
        query.setParameter("useCase", useCase);
        List<AttributeCustomization> results = query.list();
        if (results.size() == 0) {
            return null;
        }
        return results.get(0);
    }
}
