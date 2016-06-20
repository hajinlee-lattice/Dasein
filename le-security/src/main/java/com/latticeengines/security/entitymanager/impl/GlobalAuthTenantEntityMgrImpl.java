package com.latticeengines.security.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.security.GlobalAuthTenant;
import com.latticeengines.security.dao.GlobalAuthTenantDao;
import com.latticeengines.security.entitymanager.GlobalAuthTenantEntityMgr;

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

}
