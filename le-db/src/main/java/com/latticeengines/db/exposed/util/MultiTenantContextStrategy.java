package com.latticeengines.db.exposed.util;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;

public interface MultiTenantContextStrategy {

    Tenant getTenant();

    Session getSession();

    void setTenant(Tenant tenant);
}
