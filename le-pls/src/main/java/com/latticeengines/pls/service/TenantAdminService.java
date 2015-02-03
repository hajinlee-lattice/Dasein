package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.security.Tenant;

public interface TenantAdminService {

    void addTenant(Tenant tenant);
}
