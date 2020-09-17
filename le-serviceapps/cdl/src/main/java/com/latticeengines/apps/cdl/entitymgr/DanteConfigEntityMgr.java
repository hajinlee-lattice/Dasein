package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;

public interface DanteConfigEntityMgr {

    void deleteByTenantId(String tenantId);

    DanteConfigurationDocument createOrUpdate(String tenantId, DanteConfigurationDocument danteConfig);

    DanteConfigurationDocument findByTenantId(String tenantId);
}
