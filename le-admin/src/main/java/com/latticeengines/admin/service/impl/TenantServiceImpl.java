package com.latticeengines.admin.service.impl;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

@Component("tenantService")
public class TenantServiceImpl implements TenantService {

    private final BatonService batonService = new BatonServiceImpl();

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    public TenantServiceImpl() {
    }

    @Autowired
    List<LatticeComponent> components;

    private static Map<String, LatticeComponent> componentMap = new HashMap<>();

    protected static void register(LatticeComponent component) {
        componentMap.put(component.getName(), component);
    }

    public Map<String, LatticeComponent> getRegisteredServices() {
        return componentMap;
    }

    @PostConstruct
    public void postConstruct() {
        for (LatticeComponent component : components) {
            componentMap.put(component.getName(), component);
        }

        for (Map.Entry<String, LatticeComponent> entry : componentMap.entrySet()) {
            if (!entry.getValue().doRegistration()) {
                continue;
            }
            LatticeComponent component = entry.getValue();

            ServiceProperties serviceProps = new ServiceProperties();
            serviceProps.dataVersion = 1;
            serviceProps.versionString = component.getVersionString();
            ServiceInfo serviceInfo = new ServiceInfo(serviceProps, //
                    component.getInstaller(), //
                    component.getUpgrader(), //
                    null);
            ServiceWarden.registerService(component.getName(), serviceInfo);
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
    public Set<String> getRegisteredServiceKeySet() {
        return getRegisteredServices().keySet();
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

    @Override
    public SerializableDocumentDirectory getDefaultServiceConfig(String serviceName) {
        DocumentDirectory dir = batonService.getDefaultConfiguration(serviceName);
        if (dir != null) {
            SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(dir);
            DocumentDirectory metaDir = batonService.getConfigurationSchema(serviceName);
            sDir.applyMetadata(metaDir);
            return sDir;
        }
        return null;
    }

    @Override
    public SerializableDocumentDirectory getTenantServiceConfig(String contractId, String tenantId, String serviceName) {
        return tenantEntityMgr.getTenantServiceConfig(contractId, tenantId, serviceName);
    }
}
