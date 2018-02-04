package com.latticeengines.db.exposed.entitymgr;

import com.latticeengines.domain.exposed.security.Tenant;

public interface TenantEntityMgr extends BaseEntityMgrRepository<Tenant, Long> {

    Tenant findByTenantPid(Long tenantPid);

    Tenant findByTenantId(String tenantId);

    Tenant findByTenantName(String tenantName);
}
