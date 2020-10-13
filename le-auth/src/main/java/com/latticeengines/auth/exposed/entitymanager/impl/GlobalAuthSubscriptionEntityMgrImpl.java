package com.latticeengines.auth.exposed.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.auth.exposed.dao.GlobalAuthSubscriptionDao;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthSubscriptionEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthSubscription;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;

@Component("globalAuthSubscription")
public class GlobalAuthSubscriptionEntityMgrImpl extends BaseEntityMgrImpl<GlobalAuthSubscription>
        implements GlobalAuthSubscriptionEntityMgr {

    @Inject
    private GlobalAuthSubscriptionDao gaSubscriptionDao;

    @Override
    public BaseDao<GlobalAuthSubscription> getDao() {
        return gaSubscriptionDao;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthSubscription findByEmailAndTenantId(String email, String tenantId) {
        return gaSubscriptionDao.findByEmailAndTenantId(email, tenantId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthSubscription findByUserTenantRight(GlobalAuthUserTenantRight userTenantRight) {
        return gaSubscriptionDao.findByField("UserTenantRight_ID", userTenantRight.getPid());
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<String> findEmailsByTenantId(String tenantId) {
        return gaSubscriptionDao.findEmailsByTenantId(tenantId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void create(GlobalAuthSubscription gaSubscription) {
        gaSubscriptionDao.create(gaSubscription);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void create(List<GlobalAuthSubscription> gaSubscriptions) {
        if (CollectionUtils.isNotEmpty(gaSubscriptions)) {
            gaSubscriptionDao.create(gaSubscriptions);
        }
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void delete(GlobalAuthSubscription gaSubscription) {
        gaSubscriptionDao.delete(gaSubscription);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<String> getAllTenantId() {
        return gaSubscriptionDao.getAllTenantId();
    }
}
