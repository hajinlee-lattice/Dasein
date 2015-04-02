package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.security.Tenant;

public interface TenantService {

    void registerTenant(Tenant tenant);

    void discardTenant(Tenant tenant);
    
    List<Tenant> getAllTenants();
}
