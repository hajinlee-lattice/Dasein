package com.latticeengines.playmaker.entitymgr.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.dao.PalymakerTenantDao;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;

@Component("playmakerTenantEntityMgr")
public class PlaymakerTenantEntityMgrImpl implements PlaymakerTenantEntityMgr {

    @SuppressWarnings("unused")
    private final Log log = LogFactory.getLog(this.getClass());

    @Autowired
    private PalymakerTenantDao tenantDao;

    @Override
    @Transactional(value = "playmaker")
    public void executeUpdate(PlaymakerTenant tenant) {
        tenantDao.update(tenant);
    }

    @Override
    @Transactional(value = "playmaker")
    public void create(PlaymakerTenant tenant) {
        tenantDao.create(tenant);
    }

    @Override
    @Transactional(value = "playmaker")
    public void delete(PlaymakerTenant tenant) {
        tenantDao.delete(tenant);
    }

    @Override
    @Transactional(value = "playmaker")
    public PlaymakerTenant findByKey(PlaymakerTenant tenant) {
        return tenantDao.findByKey(tenant);
    }

    @Override
    @Transactional(value = "playmaker")
    public PlaymakerTenant findByTenantName(String tenantName) {
        return tenantDao.findByTenantName(tenantName);
    }

    @Override
    @Transactional(value = "playmaker")
    public boolean deleteByTenantName(String tenantName) {
        return tenantDao.deleteByTenantName(tenantName);
    }

    @Override
    @Transactional(value = "playmaker")
    public void updateByTenantName(PlaymakerTenant tenant) {
        tenantDao.updateByTenantName(tenant);

    }
}
