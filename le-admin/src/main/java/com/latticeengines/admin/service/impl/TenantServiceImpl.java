package com.latticeengines.admin.service.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

@Component("tenantService")
public class TenantServiceImpl implements TenantService {

    private final BatonService batonService = new BatonServiceImpl();

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ServiceService serviceService;

    public TenantServiceImpl() {
    }

    @Override
    public boolean createTenant(String contractId, String tenantId, TenantRegistration tenantRegistration) {
        ContractInfo contractInfo = tenantRegistration.getContractInfo();
        TenantInfo tenantInfo = tenantRegistration.getTenantInfo();
        CustomerSpaceInfo spaceInfo = tenantRegistration.getSpaceInfo();

        boolean tenantCreationSuccess =
                tenantEntityMgr.createTenant(contractId, tenantId, contractInfo, tenantInfo, spaceInfo);

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
            serviceBootstrapSuccess = serviceBootstrapSuccess
                    && bootstrap(contractId, tenantId, serviceName, flatDir);
        }
        if (!serviceBootstrapSuccess) {
            tenantEntityMgr.deleteTenant(contractId, tenantId);
            return false;
        }
        return true;
    }

    @Override
    public Collection<TenantDocument> getTenants(String contractId) {
        Collection<TenantDocument> tenants = tenantEntityMgr.getTenants(contractId);
        if (tenants != null) {
            for (TenantDocument doc :  tenants) {
                String cId = doc.getSpace().getContractId();
                String tId = doc.getSpace().getTenantId();
                doc.setBootstrapState(getTenantOverallState(cId, tId));
            }
            return tenants;
        }
        return null;
    }

    @Override
    public boolean deleteTenant(String contractId, String tenantId) {
        return tenantEntityMgr.deleteTenant(contractId, tenantId);
    }

    @Override
    public TenantDocument getTenant(String contractId, String tenantId) {
        return tenantEntityMgr.getTenant(contractId, tenantId);
    }

    @Override
    public BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName) {
        return tenantEntityMgr.getTenantServiceState(contractId, tenantId, serviceName);
    }

    @Override
    public BootstrapState getTenantOverallState(String contractId, String tenantId) {
        Set<String> components = serviceService.getRegisteredServices();
        BootstrapState state =  BootstrapState.createInitialState();
        for (String serviceName : components) {
            BootstrapState newState = tenantEntityMgr.getTenantServiceState(contractId, tenantId, serviceName);
            state = mergeBootstrapStates(state, newState, serviceName);
        }
        return state;
    }

    @Override
    public boolean bootstrap(String contractId, String tenantId, String serviceName, Map<String, String> properties) {
        return batonService.bootstrap(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, serviceName,
                properties);
    }

    @Override
    public SerializableDocumentDirectory getTenantServiceConfig(String contractId, String tenantId, String serviceName) {
        SerializableDocumentDirectory rawDir = tenantEntityMgr.getTenantServiceConfig(contractId, tenantId, serviceName);
        DocumentDirectory metaDir = serviceService.getConfigurationSchema(serviceName);
        rawDir.applyMetadata(metaDir);
        return rawDir;
    }

    private static BootstrapState mergeBootstrapStates(BootstrapState state1, BootstrapState state2, String serviceName) {
        if (state1.state.equals(BootstrapState.State.ERROR) || state2.state.equals(BootstrapState.State.ERROR))
            return BootstrapState.constructErrorState(
                    0, 0, "At least one of the components encountered an error : " + serviceName);

        if (state1.state.equals(BootstrapState.State.INITIAL) || state2.state.equals(BootstrapState.State.INITIAL))
            return BootstrapState.createInitialState();

        return BootstrapState.constructOKState(state2.installedVersion);
    }
}
