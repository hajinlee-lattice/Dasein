package com.latticeengines.app.exposed.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.app.exposed.dao.AttributeCustomizationPropertyDao;
import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationPropertyEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeCustomizationProperty;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("attributeCustomizationPropertyEntityMgr")
public class AttributeCustomizationPropertyEntityMgrImpl extends BaseEntityMgrImpl<AttributeCustomizationProperty> implements
        AttributeCustomizationPropertyEntityMgr {

    @Autowired
    private AttributeCustomizationPropertyDao attributeCustomizationPropertyDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public AttributeCustomizationPropertyDao getDao() {
        return attributeCustomizationPropertyDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public AttributeCustomizationProperty find(String name, AttributeUseCase useCase, String propertyName) {
        return getDao().find(name, useCase, propertyName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public List<AttributeCustomizationProperty> find(String name) {
        return getDao().findAllByField("attributeName", name);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.READ_COMMITTED)
    public void createOrUpdate(AttributeCustomizationProperty customization) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        customization.setTenant(tenant);
        customization.setPid(null);

        AttributeCustomizationProperty existing = find(customization.getName(), customization.getUseCase(), customization.getPropertyName());
        if (existing == null) {
            super.create(customization);
        } else {
            existing.setPropertyValue(customization.getPropertyValue());
            super.update(existing);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.READ_COMMITTED)
    public void deleteSubcategory(String categoryName, AttributeUseCase useCase, String propertyName) {
        attributeCustomizationPropertyDao.deleteSubcategory(categoryName, useCase, propertyName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.READ_COMMITTED)
    public void deleteCategory(Category category, AttributeUseCase useCase, String propertyName) {
        attributeCustomizationPropertyDao.deleteCategory(category, useCase, propertyName);
    }
}
