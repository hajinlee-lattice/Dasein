package com.latticeengines.pls.service.impl;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.CrmConfig;
import com.latticeengines.pls.service.CrmConfigService;
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Component("crmConfigService")
@Lazy(value = true)
public class CrmConfigServiceImpl implements CrmConfigService {

    @Inject
    TenantConfigServiceImpl tenantConfigService;

    @Inject
    DataLoaderService dataLoaderService;

    @Override
    public void config(String crmType, String tenantId, CrmConfig crmConfig) {
        String dlUrl = tenantConfigService.getDLRestServiceAddress(tenantId);
        dataLoaderService.updateDataProvider(crmType, tenantId, crmConfig, dlUrl);
    }
}
