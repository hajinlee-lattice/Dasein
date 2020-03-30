package com.latticeengines.db.exposed.util;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;

public class ThreadLocalMultiTenantContextStrategy implements MultiTenantContextStrategy {

    private ThreadLocal<Tenant> tenantInContext = new ThreadLocal<>();

    @Override
    public Tenant getTenant() {
        return tenantInContext.get();
    }

    @Override
    public Session getSession() {
        return null;
    }

    @Override
    public void setTenant(Tenant tenant) {
        tenantInContext.set(tenant);
    }

    @Override
    public void clear() {
        tenantInContext.set(null);
    }

    @Override
    public User getUser() {
        return null;
    }

}
