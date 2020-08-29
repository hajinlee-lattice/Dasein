package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;

public interface DanteConfigService {

    void deleteByTenant();

    List<DanteConfigurationDocument> findByTenant();

    DanteConfigurationDocument getDanteConfigByTenantId();

    DanteConfigurationDocument createAndUpdateDanteConfig();

    DanteConfigurationDocument generateDanteConfig();
}
