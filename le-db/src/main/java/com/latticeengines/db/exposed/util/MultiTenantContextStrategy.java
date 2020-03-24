package com.latticeengines.db.exposed.util;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;

public interface MultiTenantContextStrategy {

    Tenant getTenant();

    Session getSession();

    void setTenant(Tenant tenant);

    void clear();

    User getUser();
}
