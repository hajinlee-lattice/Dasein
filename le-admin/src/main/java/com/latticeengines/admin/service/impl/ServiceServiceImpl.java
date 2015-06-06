package com.latticeengines.admin.service.impl;

import java.io.IOException;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.entitymgr.ServiceEntityMgr;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

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
    public SelectableConfigurationDocument getSelectableConfigurationFields(String serviceName, boolean includeDynamicOpts) {
        if (getRegisteredServices().contains(serviceName)) {
            LatticeComponent component = orchestrator.getComponent(serviceName);
            SerializableDocumentDirectory confDir = component.getSerializableDefaultConfiguration();

            SelectableConfigurationDocument doc = new SelectableConfigurationDocument();
            doc.setComponent(serviceName);
            doc.setNodes(confDir.findSelectableFields(includeDynamicOpts));

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
            doc.setNodes(sDir.findSelectableFields(includeDynamicOpts));

            return doc;
        } else {
            return null;
        }
    }

    @Override
    public Boolean patchOptions(String serviceName, SelectableConfigurationField field) {
        try {
            SerializableDocumentDirectory conf = getDefaultServiceConfig(serviceName);
            DocumentDirectory metaDir = getConfigurationSchema(serviceName);
            conf.applyMetadata(metaDir);
            field.patch(conf);
            DocumentDirectory dir = conf.getMetadataAsDirectory();
            Path schemaPath = PathBuilder.buildServiceConfigSchemaPath(CamilleEnvironment.getPodId(), serviceName);
            dir.makePathsLocal();
            return batonService.loadDirectory(dir, schemaPath);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19101, String.format(
                    "Failed to patch options for node %s in component %s", field.getNode(), serviceName), e);
        }

    }

    @Override
    public Boolean patchDefaultConfigWithOptions(String serviceName, SelectableConfigurationField field) {
        if (!field.defaultIsValid()) {
            throw new LedpException(LedpCode.LEDP_19104, new String[]{field.getDefaultOption(),
                    field.getOptions().toString()});
        }
        try {
            SerializableDocumentDirectory conf = patchDefaultDocDir(serviceName, field);
            DocumentDirectory metaDir = patchSchemaDocDir(serviceName, field);
            conf.applyMetadata(metaDir);
            return upsertPatchedConfDir(serviceName, conf) && patchOptions(serviceName, field);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19101, String.format(
                    "Failed to patch default configuration for node %s in component %s", field.getNode(), serviceName), e);
        }
    }

    @Override
    public Boolean patchDefaultConfig(String serviceName, String nodePath, String data) {
        try {
            SerializableDocumentDirectory conf = getDefaultServiceConfig(serviceName);
            SerializableDocumentDirectory.Node node = conf.getNodeAtPath(nodePath);
            if (node == null) { throw new IllegalArgumentException("Cannot find node at path " + nodePath); }
            node.setData(data);
            DocumentDirectory metaDir = getConfigurationSchema(serviceName);
            conf.applyMetadata(metaDir);

            DocumentDirectory dirToUpload = SerializableDocumentDirectory.deserialize(conf);
            Path configPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), serviceName);
            dirToUpload.makePathsLocal();
            return batonService.loadDirectory(dirToUpload, configPath);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19101, String.format(
                    "Failed to patch default configuration for node %s in component %s", nodePath, serviceName), e);
        }
    }

    private DocumentDirectory patchSchemaDocDir(String serviceName, SelectableConfigurationField field)
            throws IOException {
        DocumentDirectory metaDir = getConfigurationSchema(serviceName);
        DocumentDirectory.Node metaNode = metaDir.get(field.getNode());
        ObjectMapper mapper = new ObjectMapper();
        metaNode.getDocument().setData(mapper.writeValueAsString(field.getOptions()));
        return metaDir;
    }

    private SerializableDocumentDirectory patchDefaultDocDir(String serviceName, SelectableConfigurationField field) {
        String nodePath = field.getNode();
        SerializableDocumentDirectory conf = getDefaultServiceConfig(serviceName);
        SerializableDocumentDirectory.Node node = conf.getNodeAtPath(nodePath);
        if (node == null) { throw new IllegalArgumentException("Cannot find node at path " + nodePath); }
        node.setData(field.getDefaultOption());
        return conf;
    }

    private boolean upsertPatchedConfDir(String serviceName, SerializableDocumentDirectory conf) {
        DocumentDirectory dirToUpload = SerializableDocumentDirectory.deserialize(conf);
        Path configPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), serviceName);
        dirToUpload.makePathsLocal();
        return batonService.loadDirectory(dirToUpload, configPath);
    }

}
