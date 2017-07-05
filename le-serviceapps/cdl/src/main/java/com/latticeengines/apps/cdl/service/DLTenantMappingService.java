package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.dataloader.DLTenantMapping;

public interface DLTenantMappingService {

    DLTenantMapping getDLTenantMapping(String dlTenantId, String dlLoadGroup);
}
