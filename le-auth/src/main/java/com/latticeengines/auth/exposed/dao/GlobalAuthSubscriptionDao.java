package com.latticeengines.auth.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthSubscription;

public interface GlobalAuthSubscriptionDao extends BaseDao<GlobalAuthSubscription> {

    GlobalAuthSubscription findByEmailAndTenantId(String email, String tenantId);

    List<String> findEmailsByTenantId(String tenantId);

    List<String> getAllTenantId();
}
