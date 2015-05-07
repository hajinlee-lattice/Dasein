package com.latticeengines.admin.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.ServiceEntityMgr;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

@Component("serviceService")
public class ServiceServiceImpl implements ServiceService {

    @Autowired
    private ServiceEntityMgr serviceEntityMgr;

    public ServiceServiceImpl() {
    }

    @Autowired
    private ComponentOrchestrator orchestrator;

    @Override
    public Set<String> getRegisteredServices() { return orchestrator.getServiceNames(); }

    @Override
    public SerializableDocumentDirectory getDefaultServiceConfig(String serviceName) {
        return serviceEntityMgr.getDefaultServiceConfig(serviceName);
    }

    @Override
    public DocumentDirectory getConfigurationSchema(String serviceName) {
        return serviceEntityMgr.getConfigurationSchema(serviceName);
    }

    @Override
    public List<SelectableConfigurationField> getSelectableConfigurationFields(String serviceName) {
        if (getRegisteredServices().contains(serviceName)) {
            LatticeComponent component = orchestrator.getComponent(serviceName);
            SerializableDocumentDirectory confDir = component.getSerializableDefaultConfiguration();
            return confDir.findSelectableFields();
        } else {
            return new ArrayList<>();
        }
    }
}
