package com.latticeengines.auth.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;

public interface GlobalAuthTenantEntityMgr extends BaseEntityMgr<GlobalAuthTenant> {

    GlobalAuthTenant findByTenantId(String tenantId);

    GlobalAuthTenant findByTenantName(String tenantName);

    GlobalAuthTenant findById(Long id);

    List<GlobalAuthTenant> findTenantNotInTenantRight(GlobalAuthUser user);
}
