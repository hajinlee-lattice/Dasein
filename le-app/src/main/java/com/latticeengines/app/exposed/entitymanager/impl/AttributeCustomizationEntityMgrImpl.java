package com.latticeengines.app.exposed.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.app.exposed.dao.AttributeCustomizationDao;
import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.AttributeCustomization;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("attributeCustomizationEntityMgr")
public class AttributeCustomizationEntityMgrImpl extends BaseEntityMgrImpl<AttributeCustomization> implements
        AttributeCustomizationEntityMgr {

    @Autowired
    private AttributeCustomizationDao attributeCustomizationDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public AttributeCustomizationDao getDao() {
        return attributeCustomizationDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AttributeCustomization find(String name, AttributeUseCase useCase) {
        return getDao().find(name, useCase);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AttributeCustomization> find(String name) {
        return getDao().findAllByField("attributeName", name);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createOrUpdate(AttributeCustomization customization) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        customization.setTenant(tenant);
        customization.setPid(null);
        super.createOrUpdate(customization);
    }

}
