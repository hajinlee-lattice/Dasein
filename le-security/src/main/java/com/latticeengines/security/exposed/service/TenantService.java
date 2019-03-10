package com.latticeengines.security.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.security.Tenant;
<<<<<<< 4d6afe0b5f92e8abd0b4cc91823b8feb407296ae
import com.latticeengines.domain.exposed.security.TenantStatus;
=======
>>>>>>> new changes on POC tenant clean up process
import com.latticeengines.domain.exposed.security.TenantType;

public interface TenantService {

    void registerTenant(Tenant tenant);

    void registerTenant(Tenant tenant, String userName);

    void updateTenant(Tenant tenant);

    void updateTenantEmailFlag(String tenantId, boolean emailSent);

    void discardTenant(Tenant tenant);
    
    List<Tenant> getAllTenants();

    List<Tenant> getTenantsByStatus(TenantStatus status);

    List<Tenant> getTenantByType(TenantType type);

    boolean getTenantEmailFlag(String tenantId);

    boolean hasTenantId(String tenantId);

    Tenant findByTenantId(String tenantId);

    Tenant findByTenantName(String tenantName);

    List<Tenant> findByTenantType(TenantType poc);
}
