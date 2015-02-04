package com.latticeengines.domain.exposed.security;

public interface HasTenant {

    void setTenant(Tenant tenant);
    
    Tenant getTenant();
    
    
}
