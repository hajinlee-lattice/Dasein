package com.latticeengines.domain.exposed.security;

public interface HasTenant {

    Tenant getTenant();

    void setTenant(Tenant tenant);

}
