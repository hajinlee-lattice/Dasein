package com.latticeengines.admin.service.impl;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.ServiceEntityMgr;
import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

@Component("tenantService")
public class TenantServiceImpl implements TenantService {

    private final BatonService batonService = new BatonServiceImpl();

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ServiceEntityMgr serviceEntityMgr;

    public TenantServiceImpl() {
    }

    @Override
    public Boolean createTenant(String contractId, String tenantId, TenantRegistration tenantRegistration) {
        CustomerSpaceInfo spaceInfo = tenantRegistration.getSpaceInfo();
        boolean tenantCreationSuccess = tenantEntityMgr.createTenant(contractId, tenantId, spaceInfo);
        if (!tenantCreationSuccess) {
            tenantEntityMgr.deleteTenant(contractId, tenantId);
            return false;
        }
        List<SerializableDocumentDirectory> configSDirs = tenantRegistration.getConfigDirectories();
        if (configSDirs == null) { return true; }
        boolean serviceBootstrapSuccess = true;
        for (SerializableDocumentDirectory configSDir: configSDirs) {
            String serviceName = configSDir.getRootPath().substring(1);
            Map<String, String> flatDir = configSDir.flatten();
            serviceBootstrapSuccess = serviceBootstrapSuccess &&
                    bootstrap(contractId, tenantId, serviceName, flatDir);
        }
        if (!serviceBootstrapSuccess) {
            tenantEntityMgr.deleteTenant(contractId, tenantId);
            return false;
        }
        return true;
    }

    @Override
    public List<SimpleEntry<String, TenantInfo>> getTenants(String contractId) {
        return tenantEntityMgr.getTenants(contractId);
    }

    @Override
    public Boolean deleteTenant(String contractId, String tenantId) {
        return tenantEntityMgr.deleteTenant(contractId, tenantId);
    }

    @Override
    public BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName) {
        return tenantEntityMgr.getTenantServiceState(contractId, tenantId, serviceName);
    }

    @Override
    public Boolean bootstrap(String contractId, String tenantId, String serviceName, Map<String, String> properties) {
        return batonService.bootstrap(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, serviceName,
                properties);
    }

    @Override
    public SerializableDocumentDirectory getTenantServiceConfig(String contractId, String tenantId, String serviceName) {
        SerializableDocumentDirectory rawDir = tenantEntityMgr.getTenantServiceConfig(contractId, tenantId, serviceName);
        DocumentDirectory metaDir = serviceEntityMgr.getConfigurationSchema(serviceName);
        rawDir.applyMetadata(metaDir);
        return rawDir;
    }
}
