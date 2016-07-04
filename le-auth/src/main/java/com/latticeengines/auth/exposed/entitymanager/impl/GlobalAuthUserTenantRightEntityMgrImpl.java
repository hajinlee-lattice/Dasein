package com.latticeengines.auth.exposed.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.auth.exposed.dao.GlobalAuthUserTenantRightDao;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantRightEntityMgr;

@Component("globalAuthUserTenantRightEntityMgr")
public class GlobalAuthUserTenantRightEntityMgrImpl extends
        BaseEntityMgrImpl<GlobalAuthUserTenantRight> implements
        GlobalAuthUserTenantRightEntityMgr {

    @Autowired
    private GlobalAuthUserTenantRightDao gaUserTenantRightDao;

    @Override
    public BaseDao<GlobalAuthUserTenantRight> getDao() {
        return gaUserTenantRightDao;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthUserTenantRight> findByUserIdAndTenantId(Long userId, Long tenantId) {
        return gaUserTenantRightDao.findByUserIdAndTenantId(userId, tenantId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthUserTenantRight findByUserIdAndTenantIdAndOperationName(Long userId,
            Long tenantId, String operationName) {
        return gaUserTenantRightDao.findByUserIdAndTenantIdAndOperationName(userId, tenantId,
                operationName);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void create(GlobalAuthUserTenantRight gaUserTenantRight) {
        super.create(gaUserTenantRight);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void delete(GlobalAuthUserTenantRight gaUserTenantRight) {
        super.delete(gaUserTenantRight);
    }

}
