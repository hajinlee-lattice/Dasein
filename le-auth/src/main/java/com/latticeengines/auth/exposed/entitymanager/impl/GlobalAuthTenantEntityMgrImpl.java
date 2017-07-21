package com.latticeengines.auth.exposed.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.auth.exposed.dao.GlobalAuthTenantDao;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTenantEntityMgr;

@Component("globalAuthTenantEntityMgr")
public class GlobalAuthTenantEntityMgrImpl extends BaseEntityMgrImpl<GlobalAuthTenant> implements
        GlobalAuthTenantEntityMgr {

    @Autowired
    private GlobalAuthTenantDao gaTenantDao;

    @Override
    public BaseDao<GlobalAuthTenant> getDao() {
        return gaTenantDao;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthTenant findByTenantId(String tenantId) {
        return gaTenantDao.findByField("Deployment_ID", tenantId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthTenant findByTenantName(String tenantName) {
        return gaTenantDao.findByField("Display_Name", tenantName);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthTenant findById(Long id) {
        return gaTenantDao.findByField("GlobalTenant_ID", id);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void create(GlobalAuthTenant gaTenant) {
        super.create(gaTenant);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void delete(GlobalAuthTenant gaTenant) {
        super.delete(gaTenant);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthTenant> findTenantNotInTenantRight(GlobalAuthUser user) {
        return gaTenantDao.findTenantNotInTenantRight(user);
    }

}
