package com.latticeengines.security.exposed.globalauth;

import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.security.Tenant;

public interface GlobalTenantManagementService {

    boolean registerTenant(Tenant tenant);

    boolean registerTenant(Tenant tenant, String userName);

    boolean discardTenant(Tenant tenant);

    boolean tenantExists(Tenant tenant);

    GlobalAuthTenant findByTenantId(String tenantId);
}
