package com.latticeengines.admin.service.impl;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

@Component("tenantService")
public class TenantServiceImpl implements TenantService {

    @Autowired
    private BatonService batonService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    public TenantServiceImpl() {
        Map<String, LatticeComponent> components = LatticeComponent.getRegisteredServices();

        for (Map.Entry<String, LatticeComponent> entry : components.entrySet()) {
            CustomerSpaceServiceBootstrapManager.register(entry.getKey(), //
                    entry.getValue().getInstaller(), //
                    entry.getValue().getUpgrader());
        }
    }

    @Override
    public Boolean createTenant(String contractId, String tenantId, CustomerSpaceInfo customerSpaceInfo) {
        return tenantEntityMgr.createTenant(contractId, tenantId, customerSpaceInfo);
    }

    @Override
    public List<SimpleEntry<String, TenantInfo>> getTenants(String contractId) {
        return tenantEntityMgr.getTenants(contractId);
    }

    @Override
    public Boolean deleteTenant(String contractId, String tenantId) {
        return tenantEntityMgr.deleteTenant(contractId, tenantId);
    }

    @Override
    public Set<String> getRegisteredServices() {
        return LatticeComponent.getRegisteredServices().keySet();
    }

    @Override
    public BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName) {
        return tenantEntityMgr.getTenantServiceState(contractId, tenantId, serviceName);
    }

    @Override
    public Boolean bootstrap(String contractId, String tenantId, String serviceName, Map<String, String> properties) {
        return batonService.bootstrap(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, serviceName,
                properties);
    }

}
