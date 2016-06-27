package com.latticeengines.security.entitymanager.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.security.dao.GlobalAuthUserDao;
import com.latticeengines.security.entitymanager.GlobalAuthUserEntityMgr;

@Component("globalAuthUserEntityMgr")
public class GlobalAuthUserEntityMgrImpl extends BaseEntityMgrImpl<GlobalAuthUser> implements GlobalAuthUserEntityMgr {

    @Autowired
    private Logger log = Logger.getLogger(GlobalAuthUserEntityMgrImpl.class);

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
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthUser> findByTenantIdJoinAuthenticationJoinUserTenantRight(Long tenantId) {
        return gaUserDao.findByTenantIdJoinAuthenticationJoinUserTenantRight(tenantId);
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
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthUser findByUserIdWithTenantRightsAndAuthentications(Long userId) {
        return gaUserDao.findByUserIdWithTenantRightsAndAuthentications(userId);
    }
}
