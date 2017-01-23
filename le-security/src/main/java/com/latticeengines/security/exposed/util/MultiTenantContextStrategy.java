package com.latticeengines.security.exposed.util;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;

public abstract class MultiTenantContextStrategy {
    public abstract Tenant getTenant();

    public abstract Session getSession();

    public abstract void setTenant(Tenant tenant);
}
