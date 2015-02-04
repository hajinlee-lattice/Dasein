package com.latticeengines.pls.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.TenantDao;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;

@Component("tenantEntityMgr")
public class TenantEntityMgrImpl extends BaseEntityMgrImpl<Tenant> implements TenantEntityMgr {

    @Autowired
    private TenantDao tenantDao;
    
    @Override
    public BaseDao<Tenant> getDao() {
        return tenantDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Tenant findByTenantId(String tenantId) {
        return tenantDao.findByTenantId(tenantId);
    }
    
}
