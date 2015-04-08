package com.latticeengines.admin.entitymgr.impl;

import java.util.AbstractMap;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

@Component("tenantEntityMgr")
public class TenantEntityMgrImpl implements TenantEntityMgr {

    @Autowired
    private BatonService batonService;
    
    @Override
    public Boolean createTenant(String contractId, String tenantId, CustomerSpaceInfo customerSpaceInfo) {
        return batonService.createTenant(contractId, //
                tenantId, //
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, //
                customerSpaceInfo);
    }

    public List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(String contractId) {
        return batonService.getTenants(contractId);
    }

    @Override
    public Boolean deleteTenant(String contractId, String tenantId) {
        return batonService.deleteTenant(contractId, tenantId);
    }

    @Override
    public BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName) {
        return batonService.getTenantServiceBootstrapState(contractId, tenantId, serviceName);
    }

    @Override
    public SerializableDocumentDirectory getTenantServiceConfig(String contractId, String tenantId, String serviceName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SerializableDocumentDirectory getDefaultTenantServiceConfig(String serviceName) {
        DocumentDirectory dir = batonService.getDefaultConfiguration(serviceName);
        if (dir == null) {
            return null;
        }
        return new SerializableDocumentDirectory(dir);
    }

}
