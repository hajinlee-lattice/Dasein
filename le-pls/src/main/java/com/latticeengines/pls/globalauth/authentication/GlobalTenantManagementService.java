package com.latticeengines.pls.globalauth.authentication;

import com.latticeengines.domain.exposed.security.Tenant;

public interface GlobalTenantManagementService {

    Boolean registerTenant(Tenant tenant);
    
    Boolean discardTenant(Tenant tenant);
}
