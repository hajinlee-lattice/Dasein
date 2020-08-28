package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DanteConfig;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;

public interface DanteConfigService {

    void cleanupByTenant();

    void cleanupByTenantId(String tenantId);

    List<DanteConfig> findByTenant();

    List<DanteConfig> findByTenant(String tenantId);

    DanteConfig getDanteConfigByTenantId(String tenantId);

    DanteConfig createAndUpdateDanteConfig(DanteConfig danteConfig);

    DanteConfig createAndUpdateDanteConfig(String tenantId);

    DanteConfig createAndUpdateDanteConfig();

    DanteConfig generateDanteConfig(String tenantId);

    DanteConfig generateDanteConfig();
}
