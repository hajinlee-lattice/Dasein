package com.latticeengines.auth.exposed.entitymanager.impl;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.auth.exposed.dao.GlobalAuthUserDao;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserEntityMgr;

@Component("globalAuthUserEntityMgr")
public class GlobalAuthUserEntityMgrImpl extends BaseEntityMgrImpl<GlobalAuthUser> implements GlobalAuthUserEntityMgr {

    private static final Logger log = Logger.getLogger(GlobalAuthUserEntityMgrImpl.class);

    @Autowired
    private GlobalAuthUserDao gaUserDao;

    @Override
    public BaseDao<GlobalAuthUser> getDao() {
        return gaUserDao;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthUser findByUserId(Long userId) {
        return gaUserDao.findByField("GlobalUser_ID", userId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthUser findByEmailJoinAuthentication(String email) {
        return gaUserDao.findByEmailJoinAuthentication(email);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthUser> findByEmailJoinUserTenantRight(String email) {
        return gaUserDao.findByEmailJoinUserTenantRight(email);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void create(GlobalAuthUser gaUser) {
        super.create(gaUser);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void delete(GlobalAuthUser gaUser) {
        log.info(String.format("Deleting user %s (%d)", gaUser.getEmail(), gaUser.getPid()));
        super.delete(gaUser);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void update(GlobalAuthUser gaUser) {
        gaUser.setLastModificationDate(new Date(System.currentTimeMillis()));
        super.update(gaUser);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthUser findByUserIdWithTenantRightsAndAuthentications(Long userId) {
        return gaUserDao.findByUserIdWithTenantRightsAndAuthentications(userId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthUser findByEmail(String email) {
        return gaUserDao.findByField("Email", email);
    }

}
