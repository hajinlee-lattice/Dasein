package com.latticeengines.baton.exposed.service.impl;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.FileSystemGetChildrenFunction;
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

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @Override
    public Boolean createTenant(String contractId, String tenantId, String defaultSpaceId, CustomerSpaceInfo spaceInfo) {
        try {
            if (!ContractLifecycleManager.exists(contractId)) {
                log.info(String.format("Creating contract %s", contractId));
                // XXX For now
                ContractLifecycleManager.create(contractId, new ContractInfo(new ContractProperties()));
            }
            if (TenantLifecycleManager.exists(contractId, tenantId)) {
                throw new RuntimeException(String.format("Tenant %s already exists", tenantId));
            }
            // XXX For now
            TenantLifecycleManager.create(contractId, tenantId, //
                    new TenantInfo(new TenantProperties(spaceInfo.properties.displayName,
                            spaceInfo.properties.description)), //
                    defaultSpaceId, spaceInfo);
        } catch (Exception e) {
            log.error("Error creating tenant", e);
            return false;
        }

        log.info(String.format("Succesfully created tenant %s", tenantId));
        return true;
    }

    @Override
    public Boolean loadDirectory(String source, String destination) {
        String rawPath = "";
        try {
            Camille c = CamilleEnvironment.getCamille();
            String podId = CamilleEnvironment.getPodId();

            // handle case where we want root pod directory
            if (destination.equals("")) {
                rawPath = String.format("/Pods/%s", podId.substring(0, podId.length()));
            } else {
                rawPath = String.format("/Pods/%s/%s", podId, destination);
            }

            File f = new File(source);
            DocumentDirectory docDir = new DocumentDirectory(new Path("/"), new FileSystemGetChildrenFunction(f));
            Path parent = new Path(rawPath);

            c.upsertDirectory(parent, docDir, ZooDefs.Ids.OPEN_ACL_UNSAFE);

        } catch (Exception e) {
            log.error("Error loading directory", e);
            return false;
        }

        log.info(String.format("Succesfully loaded files into directory %s", rawPath));
        return true;
    }

    @Override
    public Boolean bootstrap(String contractId, String tenantId, String spaceId, String serviceName,
            Map<String, String> properties) {
        CustomerSpace space = new CustomerSpace(contractId, tenantId, spaceId);
        try {
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
    public Boolean deleteTenant(String contractId, String tenantId) {
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

}
