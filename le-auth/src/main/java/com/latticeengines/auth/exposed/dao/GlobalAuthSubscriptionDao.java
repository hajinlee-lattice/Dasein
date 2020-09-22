package com.latticeengines.auth.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthSubscription;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;

public interface GlobalAuthSubscriptionDao extends BaseDao<GlobalAuthSubscription> {

    GlobalAuthSubscription findByEmailAndTenantId(String email, String tenantId);

    List<GlobalAuthUser> findUsersByTenantId(String tenantId);

    List<String> findEmailsByTenantId(String tenantId);

}
