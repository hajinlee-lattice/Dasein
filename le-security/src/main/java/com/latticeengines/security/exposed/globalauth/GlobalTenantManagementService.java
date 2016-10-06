package com.latticeengines.security.exposed.globalauth;

import com.latticeengines.domain.exposed.security.Tenant;

public interface GlobalTenantManagementService {

    boolean registerTenant(Tenant tenant);
    
    boolean discardTenant(Tenant tenant);

    boolean tenantExists(Tenant tenant);
}
