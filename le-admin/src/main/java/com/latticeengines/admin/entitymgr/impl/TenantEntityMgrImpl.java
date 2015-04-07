package com.latticeengines.admin.entitymgr.impl;

import java.util.AbstractMap;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

@Component("tenantEntityMgr")
public class TenantEntityMgrImpl implements TenantEntityMgr {

    @Autowired
    private BatonService batonService;
    
    public List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(String contractId) {
        return batonService.getTenants(contractId);
    }

    @Override
    public Boolean deleteTenant(String contractId, String tenantId) {
        return batonService.deleteTenant(contractId, tenantId);
    }
}
