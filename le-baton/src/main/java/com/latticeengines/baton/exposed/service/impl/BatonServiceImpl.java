package com.latticeengines.baton.exposed.service.impl;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.FileSystemGetChildrenFunction;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;

public class BatonServiceImpl implements BatonService {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @Override
    public void createTenant(String contractId, String tenantId, String spaceId, CustomerSpaceInfo spaceInfo) {
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
            TenantLifecycleManager.create(contractId, tenantId, new TenantInfo(new TenantProperties()), spaceId,
                    spaceInfo);
        } catch (Exception e) {
            log.error("Error creating tenant", e);
            throw new RuntimeException("Error creating tenant", e);
        }

        log.info(String.format("Succesfully created tenant %s", tenantId));
    }

    @Override
    public void loadDirectory(String source, String destination) {
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
            throw new RuntimeException("Error creating tenant", e);
        }

        log.info(String.format("Succesfully loaded files into directory %s", rawPath));
    }

    @Override
    public void bootstrap(String contractId, String tenantId, String spaceId) {
        // TODO Auto-generated method stub

    }

    @Override
    public List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(String contractId) {
        List<AbstractMap.SimpleEntry<String, TenantInfo>> tenants = new ArrayList<>();
        try {
            CamilleEnvironment.getCamille();
            return TenantLifecycleManager.getAll(contractId);
            
        } catch (Exception e) {
            log.error("Error retrieving tenants", e);
        }
        return tenants;
    }

}
