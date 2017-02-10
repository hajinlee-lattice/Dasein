package com.latticeengines.app.exposed.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.app.exposed.dao.CategoryCustomizationPropertyDao;
import com.latticeengines.app.exposed.entitymanager.CategoryCustomizationPropertyEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.CategoryCustomizationProperty;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("categoryCustomizationPropertyEntityMgr")
public class CategoryCustomizationPropertyEntityMgrImpl
        extends BaseEntityMgrImpl<CategoryCustomizationProperty>
        implements CategoryCustomizationPropertyEntityMgr {

    @Autowired
    private CategoryCustomizationPropertyDao categoryCustomizationPropertyDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<CategoryCustomizationProperty> getDao() {
        return categoryCustomizationPropertyDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CategoryCustomizationProperty find(AttributeUseCase useCase, String categoryName,
            String propertyName) {
        return categoryCustomizationPropertyDao.find(useCase, categoryName, propertyName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createOrUpdate(CategoryCustomizationProperty customization) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        customization.setTenant(tenant);
        customization.setPid(null);

        CategoryCustomizationProperty existing = find(customization.getUseCase(),
                customization.getCategoryName(), customization.getPropertyName());
        if (existing == null) {
            super.create(customization);
        } else {
            existing.setPropertyValue(customization.getPropertyValue());
            super.update(existing);
        }
    }

}
