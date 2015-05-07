package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.admin.TenantDocument;

public interface TenantConfigService {

    String getTopology(String tenantId);

    TenantDocument getTenantDocument(String tenantId);
}
