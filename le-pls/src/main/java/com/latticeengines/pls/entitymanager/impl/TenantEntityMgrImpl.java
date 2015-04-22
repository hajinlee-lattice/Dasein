package com.latticeengines.pls.entitymanager.impl;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.TenantDao;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;

@Component("tenantEntityMgr")
public class TenantEntityMgrImpl extends BasePLSEntityMgrImpl<Tenant> implements TenantEntityMgr {

    @Autowired
    private TenantDao tenantDao;
    
    @Override
    public BaseDao<Tenant> getDao() {
        return tenantDao;
    }

    @Override
    @Transactional(value = "pls", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Tenant findByTenantId(String tenantId) {
        return tenantDao.findByTenantId(tenantId);
    }
    
    @Override
    @Transactional(value = "pls", propagation = Propagation.REQUIRED)
    public void create(Tenant tenant) {
        if (tenant.getRegisteredTime() == null) {
            tenant.setRegisteredTime(new Date().getTime());
        }
        super.create(tenant);
    }

    @Override
    @Transactional(value = "pls", propagation = Propagation.REQUIRED)
    public void delete(Tenant tenant) {
        Tenant tenant1 = findByTenantId(tenant.getId());
        super.delete(tenant1);
    }
    
}
