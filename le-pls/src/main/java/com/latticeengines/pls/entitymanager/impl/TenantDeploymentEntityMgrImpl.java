package com.latticeengines.pls.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.TenantDeployment;
import com.latticeengines.pls.dao.TenantDeploymentDao;
import com.latticeengines.pls.entitymanager.TenantDeploymentEntityMgr;

@Component("tenantDeploymentEntityMgr")
public class TenantDeploymentEntityMgrImpl extends BaseEntityMgrImpl<TenantDeployment> implements TenantDeploymentEntityMgr {

    @Autowired
    private TenantDeploymentDao tenantDeploymentDao;

    @Override
    public BaseDao<TenantDeployment> getDao() {
        return tenantDeploymentDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public TenantDeployment findByTenantId(Long tenantId) {
        return tenantDeploymentDao.findByTenantId(tenantId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public boolean deleteByTenantId(Long tenantId) {
        return tenantDeploymentDao.deleteByTenantId(tenantId);
    }
}
