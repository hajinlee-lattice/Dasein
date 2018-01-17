package com.latticeengines.security.exposed.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.security.Tenant;

public interface TenantEntityMgr extends BaseEntityMgr<Tenant> {

    Tenant findByTenantPid(Long tenantPid);

    Tenant findByTenantId(String tenantId);

    Tenant findByTenantName(String tenantName);
}
