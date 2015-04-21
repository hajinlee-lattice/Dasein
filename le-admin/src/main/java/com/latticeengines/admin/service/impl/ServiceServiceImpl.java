package com.latticeengines.admin.service.impl;

import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.ServiceEntityMgr;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

@Component("serviceService")
public class ServiceServiceImpl implements ServiceService {

    private final BatonService batonService = new BatonServiceImpl();

    @Autowired
    private ServiceEntityMgr serviceEntityMgr;

    public ServiceServiceImpl() {
    }

//    @Autowired
//    List<LatticeComponent> components;
//
//    private static Map<String, LatticeComponent> componentMap = new HashMap<>();
//
//    protected static void register(LatticeComponent component) {
//        componentMap.put(component.getName(), component);
//    }
//
//    public Map<String, LatticeComponent> getRegisteredServiceComponents() {
//        return componentMap;
//    }

//    @PostConstruct
//    public void postConstruct() {
//        for (LatticeComponent component : components) {
//            componentMap.put(component.getName(), component);
//        }
//
//        for (Map.Entry<String, LatticeComponent> entry : componentMap.entrySet()) {
//            if (!entry.getValue().doRegistration()) {
//                continue;
//            }
//            LatticeComponent component = entry.getValue();
//
//            ServiceProperties serviceProps = new ServiceProperties();
//            serviceProps.dataVersion = 1;
//            serviceProps.versionString = component.getVersionString();
//            ServiceInfo serviceInfo = new ServiceInfo(serviceProps, //
//                    component.getInstaller(), //
//                    component.getUpgrader(), //
//                    null);
//            ServiceWarden.registerService(component.getName(), serviceInfo);
//        }
//    }

    @Override
    public Set<String> getRegisteredServices() {
        return serviceEntityMgr.getRegisteredServices();
    }

    @Override
    public SerializableDocumentDirectory getDefaultServiceConfig(String serviceName) {
        return serviceEntityMgr.getDefaultServiceConfig(serviceName);
    }

    @Override
    public DocumentDirectory getConfigurationSchema(String serviceName) {
        return serviceEntityMgr.getConfigurationSchema(serviceName);
    }
}
