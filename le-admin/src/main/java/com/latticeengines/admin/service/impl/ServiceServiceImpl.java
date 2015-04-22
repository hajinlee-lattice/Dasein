package com.latticeengines.admin.service.impl;

import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.ServiceEntityMgr;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

@Component("serviceService")
public class ServiceServiceImpl implements ServiceService {

    @Autowired
    private ServiceEntityMgr serviceEntityMgr;

    public ServiceServiceImpl() {
    }

    @PostConstruct
    public void postConstruct() {
        for (Map.Entry<String, LatticeComponent> entry : LatticeComponent.getRegisteredServiceComponents().entrySet()) {
            entry.getValue().register();
        }
    }

    @Override
    public Set<String> getRegisteredServices() {
        return LatticeComponent.getRegisteredServiceComponents().keySet();
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
