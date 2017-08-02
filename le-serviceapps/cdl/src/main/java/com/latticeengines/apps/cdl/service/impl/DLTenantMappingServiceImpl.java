package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymanager.DLTenantMappingEntityMgr;
import com.latticeengines.apps.cdl.service.DLTenantMappingService;
import com.latticeengines.domain.exposed.dataloader.DLTenantMapping;

@Component("dlTenantMappingService")
public class DLTenantMappingServiceImpl implements DLTenantMappingService {

    private final DLTenantMappingEntityMgr dlTenantMappingEntityMgr;

    @Inject
    public DLTenantMappingServiceImpl(DLTenantMappingEntityMgr dlTenantMappingEntityMgr) {
        this.dlTenantMappingEntityMgr = dlTenantMappingEntityMgr;
    }

    @Override
    public DLTenantMapping getDLTenantMapping(String dlTenantId, String dlLoadGroup) {
        DLTenantMapping dlTenantMapping = dlTenantMappingEntityMgr.getDLTenantMapping(dlTenantId, dlLoadGroup);
        if (dlTenantMapping == null) {
            dlTenantMapping = dlTenantMappingEntityMgr.getDLTenantMapping(dlTenantId);
        }
        return dlTenantMapping;
    }
}
