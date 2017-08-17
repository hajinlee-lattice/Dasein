package com.latticeengines.admin.entitymgr.impl;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;

@Component("adminTenantEntityMgr")
public class TenantEntityMgrImpl implements TenantEntityMgr {
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantEntityMgrImpl.class);

    private final BatonService batonService = new BatonServiceImpl();

    @Autowired
    public GlobalTenantManagementService globalTenantManagementService;

    @Override
    public boolean createTenant(String contractId, String tenantId, ContractInfo contractInfo, TenantInfo tenantInfo,
            CustomerSpaceInfo customerSpaceInfo) {
        return batonService.createTenant(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID,
                contractInfo, tenantInfo, customerSpaceInfo);
    }

    public Collection<TenantDocument> getTenants(String contractId) {
        return batonService.getTenants(contractId);
    }

    public Collection<TenantDocument> getTenantsInCache(String contractId) {
        return batonService.getTenantsInCache(contractId);
    }

    private DocumentDirectory getSpaceConfiguration(String contractId, String tenantId) {
        Path spaceConfigPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID).append(new Path("/SpaceConfiguration"));
        return CamilleEnvironment.getCamille().getDirectory(spaceConfigPath);
    }

    @Override
    public boolean deleteTenant(String contractId, String tenantId) {
        boolean success = batonService.deleteContract(contractId);
        LOGGER.info(String.format("Deleting tenant %s with contract %s, success = %s", tenantId, contractId,
                String.valueOf(success)));
        return success;
    }

    @Override
    public TenantDocument getTenant(String contractId, String tenantId) {
        return batonService.getTenant(contractId, tenantId);
    }

    @Override
    public BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName) {
        return batonService.getTenantServiceBootstrapState(contractId, tenantId, serviceName);
    }

    @Override
    public SerializableDocumentDirectory getTenantServiceConfig(String contractId, String tenantId, String serviceName) {
        Camille c = CamilleEnvironment.getCamille();
        try {
            DocumentDirectory dir = c.getDirectory(PathBuilder.buildCustomerSpaceServicePath(
                    CamilleEnvironment.getPodId(), contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID,
                    serviceName));
            return new SerializableDocumentDirectory(dir);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public SpaceConfiguration getDefaultSpaceConfig() {
        DocumentDirectory dir = batonService.getDefaultConfiguration("SpaceConfiguration");
        if (dir != null) {
            SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(dir);
            DocumentDirectory metaDir = batonService.getConfigurationSchema("SpaceConfiguration");
            sDir.applyMetadata(metaDir);
            return new SpaceConfiguration(SerializableDocumentDirectory.deserialize(sDir));
        }
        return null;
    }
}
