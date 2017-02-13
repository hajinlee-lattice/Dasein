package com.latticeengines.app.exposed.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.dao.AttributeCustomizationPropertyDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeCustomizationProperty;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;

@Component("attributeCustomizationPropertyDao")
public class AttributeCustomizationPropertyDaoImpl extends BaseDaoImpl<AttributeCustomizationProperty>
        implements AttributeCustomizationPropertyDao {

    @Override
    protected Class<AttributeCustomizationProperty> getEntityClass() {
        return AttributeCustomizationProperty.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AttributeCustomizationProperty find(String attributeName, AttributeUseCase useCase, String propertyName) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format(
                "from %s where attributeName = :attributeName " //
                        + "and useCase = :useCase " //
                        + "and propertyName = :propertyName", //
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("attributeName", attributeName);
        query.setParameter("useCase", useCase);
        query.setParameter("propertyName", propertyName);
        List<AttributeCustomizationProperty> results = query.list();
        if (results.size() == 0) {
            return null;
        }
        return results.get(0);
    }

    @Override
    public void deleteSubcategory(String categoryName, AttributeUseCase useCase, String propertyName) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format(
                "delete from %s where categoryName = :categoryName " //
                        + "and useCase = :useCase " //
                        + "and propertyName = :propertyName", //
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("categoryName", categoryName);
        query.setParameter("useCase", useCase);
        query.setParameter("propertyName", propertyName);
        query.executeUpdate();
    }

    @Override
    public void deleteCategory(Category category, AttributeUseCase useCase, String propertyName) {
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
