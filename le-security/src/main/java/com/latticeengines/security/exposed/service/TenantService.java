package com.latticeengines.security.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.security.Tenant;

public interface TenantService {

    void registerTenant(Tenant tenant);

    void updateTenant(Tenant tenant);

    void discardTenant(Tenant tenant);
    
    List<Tenant> getAllTenants();

    boolean hasTenantId(String tenantId);

    Tenant findByTenantId(String tenantId);

    Tenant findByTenantName(String tenantName);
}
