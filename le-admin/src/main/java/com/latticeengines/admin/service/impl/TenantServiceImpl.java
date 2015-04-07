package com.latticeengines.admin.service.impl;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

@Component("tenantService")
public class TenantServiceImpl implements TenantService {
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public List<SimpleEntry<String, TenantInfo>> getTenants(String contractId) {
        return tenantEntityMgr.getTenants(contractId);
    }

    @Override
    public Boolean deleteTenant(String contractId, String tenantId) {
        return tenantEntityMgr.deleteTenant(contractId, tenantId);
    }

}
