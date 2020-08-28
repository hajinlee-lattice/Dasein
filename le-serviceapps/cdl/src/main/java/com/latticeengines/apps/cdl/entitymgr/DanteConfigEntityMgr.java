package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DanteConfig;

public interface DanteConfigEntityMgr {

    void cleanupTenant(String tenantId);

    List<DanteConfig> findAllByTenantId(String tenantId);

    DanteConfig saveAndUpdate(String tenantId, DanteConfig danteConfig);
}
