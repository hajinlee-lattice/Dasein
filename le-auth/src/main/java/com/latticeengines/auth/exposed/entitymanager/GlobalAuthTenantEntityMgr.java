package com.latticeengines.auth.exposed.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;

public interface GlobalAuthTenantEntityMgr extends BaseEntityMgr<GlobalAuthTenant> {

    GlobalAuthTenant findByTenantId(String tenantId);

    GlobalAuthTenant findByTenantName(String tenantName);

    GlobalAuthTenant findById(Long id);

}
