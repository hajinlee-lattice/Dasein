package com.latticeengines.security.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;
import com.latticeengines.security.dao.GlobalAuthAuthenticationDao;
import com.latticeengines.security.entitymanager.GlobalAuthAuthenticationEntityMgr;

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
        super.update(gaAuthentication);
    }
}
