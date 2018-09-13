package com.latticeengines.app.exposed.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.dao.CategoryCustomizationPropertyDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.CategoryCustomizationProperty;

@Component("categoryCustomizationPropertyDao")
public class CategoryCustomizationPropertyDaoImpl extends BaseDaoImpl<CategoryCustomizationProperty>
        implements CategoryCustomizationPropertyDao {

    @Override
    protected Class<CategoryCustomizationProperty> getEntityClass() {
        return CategoryCustomizationProperty.class;
    }

    @Override
    public CategoryCustomizationProperty find(AttributeUseCase useCase, String categoryName, String propertyName) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format(
                "from %s where categoryName = :categoryName " //
                        + "and useCase = :useCase " //
                        + "and propertyName = :propertyName", //
                getEntityClass().getSimpleName());
        Query<CategoryCustomizationProperty> query = session.createQuery(queryStr, CategoryCustomizationProperty.class);
        query.setParameter("categoryName", categoryName);
        query.setParameter("useCase", useCase);
        query.setParameter("propertyName", propertyName);
        List<CategoryCustomizationProperty> results = query.list();
        if (results.size() == 0) {
            return null;
        }
        return results.get(0);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void deleteSubcategories(Category category, AttributeUseCase useCase, String propertyName) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format(
                "delete from %s where categoryName like :categoryName " //
                        + "and useCase = :useCase " //
                        + "and propertyName = :propertyName", //
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("categoryName", category.getName() + ".%");
        query.setParameter("useCase", useCase);
        query.setParameter("propertyName", propertyName);
        query.executeUpdate();
    }
}
