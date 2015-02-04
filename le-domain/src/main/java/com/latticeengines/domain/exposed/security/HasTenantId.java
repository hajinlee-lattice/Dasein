package com.latticeengines.domain.exposed.security;

public interface HasTenantId {

    void setTenantId(Long tenantId);
    
    Long getTenantId();
}
