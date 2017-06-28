package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataloader.DLTenantMapping;
import com.latticeengines.pls.entitymanager.DLTenantMappingEntityMgr;
import com.latticeengines.pls.service.DLTenantMappingService;

@Component("dlTenantMappingService")
public class DLTenantMappingServiceImpl implements DLTenantMappingService {

    @Autowired
    private DLTenantMappingEntityMgr dlTenantMappingEntityMgr;

    @Override
    public DLTenantMapping getDLTenantMapping(String dlTenantId, String dlLoadGroup) {
        DLTenantMapping dlTenantMapping = dlTenantMappingEntityMgr.getDLTenantMapping(dlTenantId, dlLoadGroup);
        if (dlTenantMapping == null) {
            dlTenantMapping = dlTenantMappingEntityMgr.getDLTenantMapping(dlTenantId);
        }
        return dlTenantMapping;
    }
}
