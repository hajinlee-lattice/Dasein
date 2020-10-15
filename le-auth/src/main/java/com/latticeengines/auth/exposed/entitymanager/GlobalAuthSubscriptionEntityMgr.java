package com.latticeengines.auth.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthSubscription;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;

public interface GlobalAuthSubscriptionEntityMgr extends BaseEntityMgr<GlobalAuthSubscription> {

    List<String> findEmailsByTenantId(String tenantId);

    GlobalAuthSubscription findByEmailAndTenantId(String email, String tenantId);

    GlobalAuthSubscription findByUserTenantRight(GlobalAuthUserTenantRight userTenantRight);

    void create(List<GlobalAuthSubscription> gaSubscriptions);

    List<String> getAllTenantId();
}
