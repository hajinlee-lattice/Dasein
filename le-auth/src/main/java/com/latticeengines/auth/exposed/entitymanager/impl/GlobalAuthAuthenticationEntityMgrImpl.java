package com.latticeengines.auth.exposed.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;
import com.latticeengines.auth.exposed.dao.GlobalAuthAuthenticationDao;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthAuthenticationEntityMgr;

import java.util.Date;
import java.util.HashMap;

@Component("globalAuthAuthenticationEntityMgr")
public class GlobalAuthAuthenticationEntityMgrImpl extends
        BaseEntityMgrImpl<GlobalAuthAuthentication> implements
        GlobalAuthAuthenticationEntityMgr {

    @Autowired
    private GlobalAuthAuthenticationDao gaAuthenticationDao;

    @Override
    public BaseDao<GlobalAuthAuthentication> getDao() {
        return gaAuthenticationDao;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthAuthentication findByUsername(String username) {
        return gaAuthenticationDao.findByField("Username", username);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthAuthentication findByUsernameJoinUser(String username) {
        return gaAuthenticationDao.findByUsernameJoinUser(username);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthAuthentication findByUserId(Long userId) {
        return gaAuthenticationDao.findByField("User_ID", userId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public HashMap<Long, String> findUserInfoByTenantId(Long tenantId) {
        return gaAuthenticationDao.findUserInfoByTenantId(tenantId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void create(GlobalAuthAuthentication gaAuthentication) {
        super.create(gaAuthentication);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void delete(GlobalAuthAuthentication gaAuthentication) {
        super.delete(gaAuthentication);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void update(GlobalAuthAuthentication gaAuthentication) {
        gaAuthentication.setLastModificationDate(new Date(System.currentTimeMillis()));
        super.update(gaAuthentication);
    }

}
