package com.latticeengines.security.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
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

    void setNotificationStateByTenantId(String tenantId, String notificationLevel);

    void setNotificationTypeByTenantId(String tenantId, String notificationType);

}
