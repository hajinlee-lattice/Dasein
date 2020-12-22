package com.latticeengines.auth.exposed.entitymanager.impl;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.hibernate.Hibernate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.auth.exposed.dao.GlobalAuthUserTenantRightDao;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantRightEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;

@Component("globalAuthUserTenantRightEntityMgr")
public class GlobalAuthUserTenantRightEntityMgrImpl extends
        BaseEntityMgrImpl<GlobalAuthUserTenantRight> implements GlobalAuthUserTenantRightEntityMgr {

    @Inject
    private GlobalAuthUserTenantRightDao gaUserTenantRightDao;

    @Override
    public BaseDao<GlobalAuthUserTenantRight> getDao() {
        return gaUserTenantRightDao;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthUserTenantRight findByUserIdAndTenantId(Long userId, Long tenantId) {
        return gaUserTenantRightDao.findByUserIdAndTenantId(userId, tenantId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthUserTenantRight findByUserIdAndTenantId(Long userId, Long tenantId, boolean inflate) {
        GlobalAuthUserTenantRight globalAuthUserTenantRight = gaUserTenantRightDao.findByUserIdAndTenantId(userId, tenantId);
        if (inflate && globalAuthUserTenantRight != null) {
            inflateUserTenantRight(globalAuthUserTenantRight);
        }
        return globalAuthUserTenantRight;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthUser> findUsersByTenantId(Long tenantId) {
        return gaUserTenantRightDao.findUsersByTenantId(tenantId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthUserTenantRight findByUserIdAndTenantIdAndOperationName(Long userId,
            Long tenantId, String operationName) {
        return gaUserTenantRightDao.findByUserIdAndTenantIdAndOperationName(userId, tenantId,
                operationName);
    }

    private void inflateUserTenantRight(GlobalAuthUserTenantRight gaUserTenantRight) {
        if (gaUserTenantRight != null) {
            Hibernate.initialize(gaUserTenantRight.getGlobalAuthTeams());
        }
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthUserTenantRight findByUserIdAndTenantIdAndOperationName(Long userId,
                                                                             Long tenantId, String operationName, boolean inflate) {
        GlobalAuthUserTenantRight gaUserTenantRight = gaUserTenantRightDao.findByUserIdAndTenantIdAndOperationName(userId, tenantId, operationName);
        if (inflate) {
            inflateUserTenantRight(gaUserTenantRight);
        }
        return gaUserTenantRight;
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

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthUserTenantRight> findByEmail(String email) {
        return gaUserTenantRightDao.findByEmail(email);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public boolean isRedundant(String email) {
        return !gaUserTenantRightDao.existsByEmail(email);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthUserTenantRight> findByTenantId(Long tenantId) {
        return gaUserTenantRightDao.findAllByField("Tenant_ID", tenantId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthUserTenantRight> findByTenantId(Long tenantId, boolean inflate) {
        List<GlobalAuthUserTenantRight> globalAuthUserTenantRightList = gaUserTenantRightDao.findAllByField(
                "Tenant_ID", tenantId);
        if (inflate) {
            for (GlobalAuthUserTenantRight globalAuthUserTenantRight : globalAuthUserTenantRightList) {
                Hibernate.initialize(globalAuthUserTenantRight.getGlobalAuthTeams());
            }
        }
        return globalAuthUserTenantRightList;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public Boolean deleteByUserId(Long userId) {
        return gaUserTenantRightDao.deleteByUserId(userId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthUserTenantRight> findByNonNullExpirationDate() {
        return gaUserTenantRightDao.findByNonNullExpirationDate();
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthUserTenantRight> findByEmailsAndTenantId(Set<String> emails, Long tenantId) {
        return gaUserTenantRightDao.findByEmailsAndTenantId(emails, tenantId);
    }

}
