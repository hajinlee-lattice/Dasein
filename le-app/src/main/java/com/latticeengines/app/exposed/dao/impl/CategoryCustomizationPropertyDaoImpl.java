package com.latticeengines.app.exposed.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.dao.CategoryCustomizationPropertyDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.CategoryCustomizationProperty;

@Component("categoryCustomizationPropertyDao")
public class CategoryCustomizationPropertyDaoImpl extends BaseDaoImpl<CategoryCustomizationProperty>
        implements CategoryCustomizationPropertyDao {

    @Override
    protected Class<CategoryCustomizationProperty> getEntityClass() {
        return CategoryCustomizationProperty.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CategoryCustomizationProperty find(AttributeUseCase useCase, String categoryName,
            String propertyName) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format(
                "from %s where categoryName = :categoryName " //
                        + "and useCase = :useCase " //
                        + "and propertyName = :propertyName", //
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("categoryName", categoryName);
        query.setParameter("useCase", useCase);
        query.setParameter("propertyName", propertyName);
        List<CategoryCustomizationProperty> results = query.list();
        if (results.size() == 0) {
            return null;
        }
        return results.get(0);
    }
}
