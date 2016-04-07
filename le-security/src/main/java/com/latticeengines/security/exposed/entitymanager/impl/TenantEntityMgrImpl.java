package com.latticeengines.security.exposed.entitymanager.impl;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.dao.TenantDao;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("tenantEntityMgr")
public class TenantEntityMgrImpl extends BaseEntityMgrImpl<Tenant> implements TenantEntityMgr {

    @Autowired
    private TenantDao tenantDao;

    @Override
    public BaseDao<Tenant> getDao() {
        return tenantDao;
    }

    @Override
    @Transactional(value = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Tenant findByTenantId(String tenantId) {
        return tenantDao.findByTenantId(tenantId);
    }

    @Override
    @Transactional(value = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Tenant findByTenantName(String tenantName) {
        return tenantDao.findByTenantName(tenantName);
    }

    @Override
    @Transactional(value = "transactionManager", propagation = Propagation.REQUIRED)
    public void create(Tenant tenant) {
        if (tenant.getRegisteredTime() == null) {
            tenant.setRegisteredTime(new Date().getTime());
        }
        super.create(tenant);
    }

    @Override
    @Transactional(value = "transactionManager", propagation = Propagation.REQUIRED)
    public void delete(Tenant tenant) {
        Tenant tenant1 = findByTenantId(tenant.getId());
        super.delete(tenant1);
    }

}
