package com.latticeengines.baton.exposed.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.ZooDefs;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceBootstrapManager;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.FileSystemGetChildrenFunction;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;

public class BatonServiceImpl implements BatonService {

    private static final Logger log = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

    @Override
    public boolean createTenant(String contractId, String tenantId, String defaultSpaceId, CustomerSpaceInfo spaceInfo) {
        ContractInfo contractInfo = new ContractInfo(new ContractProperties());
        TenantInfo tenantInfo = new TenantInfo(
                new TenantProperties(spaceInfo.properties.displayName, spaceInfo.properties.description));
        return createTenant(contractId, tenantId, defaultSpaceId, contractInfo, tenantInfo, spaceInfo);
    }

    @Override
    public boolean createTenant(String contractId, String tenantId, String defaultSpaceId,
                                ContractInfo contractInfo, TenantInfo tenantInfo, CustomerSpaceInfo spaceInfo) {
        try {
            if (!ContractLifecycleManager.exists(contractId)) {
                log.info(String.format("Creating contract %s", contractId));
                // XXX For now
                ContractLifecycleManager.create(contractId, contractInfo);
            }
            // XXX For now
            if (TenantLifecycleManager.exists(contractId, tenantId)) {
                TenantLifecycleManager.delete(contractId, tenantId);
            }
            // Timestamp
            tenantInfo.properties.created = new DateTime().getMillis();
            tenantInfo.properties.lastModified = new DateTime().getMillis();

            TenantLifecycleManager.create(contractId, tenantId, tenantInfo, defaultSpaceId, spaceInfo);
        } catch (Exception e) {
            log.error("Error creating tenant", e);
            return false;
        }

        log.info(String.format("Succesfully created tenant %s", tenantId));
        return true;
    }

    @Override
    public boolean loadDirectory(String source, String destination) {
        try {
            String rawPath = "";
            String podId = CamilleEnvironment.getPodId();

            // handle case where we want root pod directory
            if (destination.equals("")) {
                rawPath = String.format("/Pods/%s", podId.substring(0, podId.length()));
            } else {
                rawPath = String.format("/Pods/%s/%s", podId, destination);
            }

            Path parent = new Path(rawPath);
            File f = new File(source);
            DocumentDirectory docDir = new DocumentDirectory(new Path("/"), new FileSystemGetChildrenFunction(f));

            return loadDirectory(docDir, parent);

        } catch (IOException e) {
            log.error("Error converting source string to file", e);
            return false;
        }
    }

    @Override
    public boolean loadDirectory(DocumentDirectory sourceDir, Path absoluteRootPath) {
        String rawPath = "";
        try {
            Camille c = CamilleEnvironment.getCamille();
            // convert paths to relative to parent
            sourceDir.makePathsLocal();
            c.upsertDirectory(absoluteRootPath, sourceDir, ZooDefs.Ids.OPEN_ACL_UNSAFE);

        } catch (Exception e) {
            log.error("Error loading directory", e);
            return false;
        }

        log.info(String.format("Succesfully loaded files into directory %s", rawPath));
        return true;
    }

    @Override
    public boolean bootstrap(String contractId, String tenantId, String spaceId, String serviceName,
            Map<String, String> properties) {
        CustomerSpace space = new CustomerSpace(contractId, tenantId, spaceId);
        try {
            log.info("Bootstrapping service " + serviceName + " in space " + space);
            ServiceWarden.commandBootstrap(serviceName, space, properties);
        } catch (Exception e) {
            log.error("Error commanding bootstrap for service " + serviceName + " and space " + space);
            return false;
        }
        return true;
    }

    @Override
    public List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(String contractId) {
        List<AbstractMap.SimpleEntry<String, TenantInfo>> tenants = new ArrayList<>();
        try {
            CamilleEnvironment.getCamille();

            if (contractId != null) {
                return TenantLifecycleManager.getAll(contractId);
            }

            List<AbstractMap.SimpleEntry<String, ContractInfo>> contracts = ContractLifecycleManager.getAll();

            for (AbstractMap.SimpleEntry<String, ContractInfo> contract : contracts) {
                tenants.addAll(TenantLifecycleManager.getAll(contract.getKey()));
            }

        } catch (Exception e) {
            log.error("Error retrieving tenants", e);
        }
        return tenants;
    }

    @Override
    public boolean deleteTenant(String contractId, String tenantId) {
        try {
            CamilleEnvironment.getCamille();
            if (TenantLifecycleManager.exists(contractId, tenantId)) {
                TenantLifecycleManager.delete(contractId, tenantId);
                return true;
            } else {
                return false;
            }

        } catch (Exception e) {
            log.error("Error retrieving tenants", e);
            return false;
        }
    }

    @Override
    public TenantInfo getTenant(String contractId, String tenantId) {
        TenantInfo tenantInfo = null;
        try {
            tenantInfo = TenantLifecycleManager.getInfo(contractId, tenantId);
            try {
                CustomerSpaceInfo spaceInfo =
                        SpaceLifecycleManager.getInfo(contractId, tenantId,
                                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
                tenantInfo.spaceInfoList = new ArrayList<>();
                tenantInfo.spaceInfoList.add(spaceInfo);
            } catch (Exception e) {
                log.error("Could not get the info of the default space for tenant " + tenantId);
            }
        } catch (Exception e) {
            log.error("Error retrieving tenant " + tenantId + " in " + contractId, e);
        }
        return tenantInfo;
    }

    @Override
    public boolean discardService(String serviceName) {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            String podId = CamilleEnvironment.getPodId();
            Path serviceRootPath = PathBuilder.buildServicePath(podId, serviceName);
            if (camille.exists(serviceRootPath)) {
                camille.delete(serviceRootPath);
                return true;
            } else {
                return false;
            }

        } catch (Exception e) {
            log.error("Error discarding service " + serviceName, e);
            return false;
        }
    }

    @Override
    public Set<String> getRegisteredServices() {
        try {
            return ServiceBootstrapManager.getRegisteredBootstrappers();
        } catch (Exception e) {
            log.error("Error getting all registered services.", e);
            return null;
        }
    }


    @Override
    public BootstrapState getTenantServiceBootstrapState(String contractId, String tenantId, String serviceName) {
        CustomerSpace customerSpace = new CustomerSpace(contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        try {
            return CustomerSpaceServiceBootstrapManager.getBootstrapState(serviceName, customerSpace);
        } catch (Exception e) {
            log.error("Error retrieving tenant service state", e);
            return null;
        }
    }

    @Override
    public DocumentDirectory getDefaultConfiguration(String serviceName) {
        try {
            return CustomerSpaceServiceBootstrapManager.getDefaultConfiguration(serviceName);
        } catch (Exception e) {
            log.error("Error retrieving default config for service " + serviceName, e);
            return null;
        }
    }

    @Override
    public DocumentDirectory getConfigurationSchema(String serviceName) {
        try {
            return CustomerSpaceServiceBootstrapManager.getConfigurationSchema(serviceName);
        } catch (Exception e) {
            log.error("Error retrieving default config for service " + serviceName, e);
            return null;
        }
    }

}
