package com.latticeengines.admin.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.ServiceEntityMgr;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;

@Component("serviceService")
public class ServiceServiceImpl implements ServiceService {

    private final BatonService batonService = new BatonServiceImpl();

    @Autowired
    private ServiceEntityMgr serviceEntityMgr;

    public ServiceServiceImpl() {
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
    public Set<String> getRegisteredServiceKeySet() {
        return getRegisteredServices().keySet();
    }

    @Override
    public SerializableDocumentDirectory getDefaultServiceConfig(String serviceName) {
        return serviceEntityMgr.getDefaultServiceConfig(serviceName);
    }
}
