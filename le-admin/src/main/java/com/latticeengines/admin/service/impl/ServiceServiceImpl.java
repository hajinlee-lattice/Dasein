package com.latticeengines.admin.service.impl;

import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.ServiceEntityMgr;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

@Component("serviceService")
public class ServiceServiceImpl implements ServiceService {

    @Autowired
    private ServiceEntityMgr serviceEntityMgr;

    private static BatonService batonService = new BatonServiceImpl();

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
    public SelectableConfigurationDocument getSelectableConfigurationFields(String serviceName) {
        if (getRegisteredServices().contains(serviceName)) {
            LatticeComponent component = orchestrator.getComponent(serviceName);
            SerializableDocumentDirectory confDir = component.getSerializableDefaultConfiguration();

            SelectableConfigurationDocument doc = new SelectableConfigurationDocument();
            doc.setComponent(serviceName);
            doc.setNodes(confDir.findSelectableFields());

            return doc;
        } else if (serviceName.equals("SpaceConfiguration")) {
            DocumentDirectory confDir = batonService.getDefaultConfiguration("SpaceConfiguration");
            DocumentDirectory metaDir = batonService.getConfigurationSchema("SpaceConfiguration");
            confDir.makePathsLocal();
            metaDir.makePathsLocal();
            SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(confDir);
            sDir.applyMetadata(metaDir);

            SelectableConfigurationDocument doc = new SelectableConfigurationDocument();
            doc.setComponent(serviceName);
            doc.setNodes(sDir.findSelectableFields());

            return doc;
        } else {
            return null;
        }
    }
}
