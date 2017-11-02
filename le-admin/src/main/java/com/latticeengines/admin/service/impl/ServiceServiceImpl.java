package com.latticeengines.admin.service.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.entitymgr.ServiceEntityMgr;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.security.exposed.AccessLevel;


@Component("serviceService")
public class ServiceServiceImpl implements ServiceService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceServiceImpl.class);

    private final String listDelimiter = ",";

    @Autowired
    private ServiceEntityMgr serviceEntityMgr;

    private static BatonService batonService = new BatonServiceImpl();

    public ServiceServiceImpl() {
    }

    @Autowired
    private ComponentOrchestrator orchestrator;

    @Override
    public Set<String> getRegisteredServices() {
        return orchestrator.getServiceNames();
    }

    @Override
    public Map<String, Set<LatticeProduct>> getRegisteredServicesWithProducts() {
        return orchestrator.getServiceNamesWithProducts();
    }

    @Override
    public SerializableDocumentDirectory getDefaultServiceConfig(String serviceName) {
        return serviceEntityMgr.getDefaultServiceConfig(serviceName);
    }

    @Override
    public DocumentDirectory getConfigurationSchema(String serviceName) {
        return serviceEntityMgr.getConfigurationSchema(serviceName);
    }

    @Override
    public SelectableConfigurationDocument getSelectableConfigurationFields(String serviceName,
            boolean includeDynamicOpts) {
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
            Camille camille = CamilleEnvironment.getCamille();

            Path schemaPath = PathBuilder.buildServiceConfigSchemaPath(CamilleEnvironment.getPodId(), serviceName);
            schemaPath = schemaPath.append(new Path(field.getNode()));

            String metaStr = null;
            try {
                metaStr = camille.get(schemaPath).getData();
            } catch (Exception e) {
                // ignore
            }

            ObjectMapper mapper = new ObjectMapper();
            SerializableDocumentDirectory.Metadata metadata = new SerializableDocumentDirectory.Metadata();
            metadata.setType("options");
            if (!StringUtils.isEmpty(metaStr)) {
                mapper.readValue(metaStr, SerializableDocumentDirectory.Metadata.class);
            }
            metadata.setOptions(field.getOptions());
            Document doc = camille.get(schemaPath);
            doc.setData(mapper.writeValueAsString(metadata));
            camille.set(schemaPath, doc);

            return true;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19101, String.format(
                    "Failed to patch options for node %s in component %s", field.getNode(), serviceName), e);
        }

    }

    @Override
    public Boolean patchDefaultConfig(String serviceName, String nodePath, String data) {
        try {
            validateBewDefaultValue(serviceName, nodePath, data);
            patchDefaultConfigWithoutValidation(serviceName, nodePath, data);
            return true;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19101, String.format(
                    "Failed to patch default configuration for node %s in component %s", nodePath, serviceName), e);
        }
    }

    @Override
    public Boolean patchDefaultConfigWithOptions(String serviceName, SelectableConfigurationField field) {
        if (!field.defaultIsValid()) {
            throw new LedpException(LedpCode.LEDP_19104, new String[] { field.getDefaultOption(),
                    field.getOptions().toString() });
        }

        try {
            patchDefaultConfigWithoutValidation(serviceName, field.getNode(), field.getDefaultOption());
            patchOptions(serviceName, field);
            return true;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19101, String.format(
                    "Failed to patch default configuration for node %s in component %s", field.getNode(), serviceName),
                    e);
        }
    }

    public Boolean patchDefaultConfigWithoutValidation(String serviceName, String nodePath, String data) {
        try {
            Path configPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), serviceName);
            configPath = configPath.append(new Path(nodePath));
            patchConfig(configPath, data);
            return true;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19101, String.format(
                    "Failed to patch default configuration for node %s in component %s", nodePath, serviceName), e);
        }
    }

    private void validateBewDefaultValue(String serviceName, String nodePath, String data) {
        SerializableDocumentDirectory conf = getDefaultServiceConfig(serviceName);
        SerializableDocumentDirectory.Node node = conf.getNodeAtPath(nodePath);
        if (node == null) {
            throw new IllegalArgumentException("Cannot find node at path " + nodePath);
        }
        node.setData(data);
        DocumentDirectory metaDir = getConfigurationSchema(serviceName);
        conf.applyMetadata(metaDir);
    }

    @Override
    public Boolean patchNewConfig(String serviceName, AccessLevel level, String emails) {
        String nodePath;
        if (level.equals(AccessLevel.INTERNAL_ADMIN)) {
            nodePath = "/LatticeAdminEmails";
        } else {
            nodePath = "/SuperAdminEmails";
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            SerializableDocumentDirectory conf = getDefaultServiceConfig(serviceName);
            SerializableDocumentDirectory.Node node = conf.getNodeAtPath(nodePath);
            String data = node.getData();
            Set<String> emailSet = new HashSet<>();
            for (String email : Arrays.asList(emails.trim().split(listDelimiter))) {
                email = email.trim();
                if(!StringUtils.isEmpty(email) && !data.contains(email)) {
                    emailSet.add(email);
                }
            }
            if(emailSet != null && emailSet.size() > 0) {
                try {
                    @SuppressWarnings("unchecked")
                    List<String> initial = mapper.readValue(data, List.class);
                    initial.addAll(emailSet);
                    data = mapper.writeValueAsString(initial);
                } catch (Exception e) {
                    throw new RuntimeException("parse data error when read or write");
                }
                patchDefaultConfigWithoutValidation(serviceName, nodePath, data);
                return true;
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19101, String.format(
                    "Failed to patch new configuration for node %s in component %s", nodePath, serviceName), e);
        }
        return false;
    }

    @Override
    public Boolean reduceConfig(String serviceName, String emails) {
        String[] nodePaths = {"/LatticeAdminEmails", "/SuperAdminEmails", "/ExternalAdminEmails", "/ThirdPartyUserEmails"};
        Set<String> emailSet = new HashSet<>();
        for (String email : Arrays.asList(emails.trim().split(listDelimiter))) {
            email = email.trim();
            if(!StringUtils.isEmpty(email)) {
                emailSet.add(email);
            }
        }
        for (String nodePath : nodePaths) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                SerializableDocumentDirectory conf = getDefaultServiceConfig(serviceName);
                SerializableDocumentDirectory.Node node = conf.getNodeAtPath(nodePath);
                String data = node.getData();
                try {
                    @SuppressWarnings("unchecked")
                    List<String> initial = mapper.readValue(data, List.class);
                    for(String email : emailSet) {
                        if(initial.contains(email)) {
                            initial.remove(email);
                        }
                    }
                    data = mapper.writeValueAsString(initial);
                } catch (Exception e) {
                    throw new RuntimeException("parse data error when read or write");
                }
                patchDefaultConfigWithoutValidation(serviceName, nodePath, data);
            } catch (Exception e) {
                LOGGER.error(String.format(
                        "Failed to patch new configuration for node %s in component %s", nodePath, serviceName), e);
            }
        }
        return true;
    }

	@Override
	public Boolean patchTenantServiceConfig(String tenantId, String serviceName, String nodePath, String data) {
		try {
            Path configPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),tenantId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, serviceName);
            configPath = configPath.append(new Path(nodePath));
            patchConfig(configPath, data);
            return true;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19101, String.format(
                    "Failed to patch configuration for node %s in component %s for tenant %s", nodePath, serviceName, tenantId), e);
        }
	}

	private void patchConfig(Path configPath, String data) throws Exception {
		Camille camille = CamilleEnvironment.getCamille();
        Document doc = camille.get(configPath);
        doc.setData(data);
        camille.set(configPath, doc);
	}

}
