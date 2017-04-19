package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.security.Tenant;

public interface TenantPurgeService {
    void purge(Tenant tenant);
}
