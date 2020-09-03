package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;

public interface DanteConfigEntityMgr {

    void deleteByTenantId(String tenantId);

    List<DanteConfigurationDocument> findAllByTenantId(String tenantId);

    DanteConfigurationDocument createOrUpdate(String tenantId, DanteConfigurationDocument danteConfig);
}
