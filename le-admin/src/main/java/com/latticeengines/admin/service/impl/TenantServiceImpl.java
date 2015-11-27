package com.latticeengines.admin.service.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.DefaultConfigOverwritter;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.dante.DanteComponent;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

@Component("adminTenantService")
public class TenantServiceImpl implements TenantService {
    private static final String spaceConfigNode = LatticeComponent.spaceConfigNode;
    private static final String danteFeatureFlag = "Dante";

    private final BatonService batonService = new BatonServiceImpl();

    @Autowired
    private DanteComponent danteComponent;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ServiceService serviceService;

    @Autowired
    private ComponentOrchestrator orchestrator;

    @Autowired
    private DefaultConfigOverwritter overwritter;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    public TenantServiceImpl() {
    }

    @PostConstruct
    protected void uploadDefaultSpaceConfigAndSchemaByJson() {
        boolean needToRegister = Boolean.valueOf(System.getProperty("com.latticeengines.registerBootstrappers"));
        if (needToRegister) {
            String defaultJson = "space_default.json";
            String metadataJson = "space_metadata.json";
            String serviceName = "SpaceConfiguration";
            LatticeComponent.uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson, serviceName);
            overwritter.overwriteDefaultSpaceConfig();
        }

    }

    @Override
    public boolean createTenant(final String contractId, final String tenantId, TenantRegistration tenantRegistration) {
        final ContractInfo contractInfo = tenantRegistration.getContractInfo();
        final TenantInfo tenantInfo = tenantRegistration.getTenantInfo();
        final CustomerSpaceInfo spaceInfo = tenantRegistration.getSpaceInfo();
        final SpaceConfiguration spaceConfig = tenantRegistration.getSpaceConfig();

        boolean tenantCreationSuccess = tenantEntityMgr.createTenant(contractId, tenantId, contractInfo, tenantInfo,
                spaceInfo);

        tenantCreationSuccess = tenantCreationSuccess && setupSpaceConfiguration(contractId, tenantId, spaceConfig);

        if (!tenantCreationSuccess) {
            tenantEntityMgr.deleteTenant(contractId, tenantId);
            return false;
        }

        // use featureFlag service to write to FFV about the feature flag values
        // (all)

        List<SerializableDocumentDirectory> configSDirs = tenantRegistration.getConfigDirectories();
        if (configSDirs == null) {
            return true;
        }
        Map<String, Map<String, String>> props = new HashMap<>();
        for (SerializableDocumentDirectory configSDir : configSDirs) {
            String serviceName = configSDir.getRootPath().substring(1);
            Map<String, String> flatDir = configSDir.flatten();
            props.put(serviceName, flatDir);
        }

        // as a short term patch, waking up Dante's .NET App Pool is necessary
        // for it to pick up the bootstrap command.
        danteComponent.wakeUpAppPool();

        // change components in orchestrator based on selected product
        // retrieve mappings from Camille
        final Map<String, Map<String, String>> orchestratorProps = props;
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                orchestrator.orchestrate(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID,
                        orchestratorProps);
            }
        });

        return true;
    }

    @Override
    public Collection<TenantDocument> getTenants(String contractId) {
        Collection<TenantDocument> tenants = tenantEntityMgr.getTenants(contractId);
        if (tenants != null) {
            for (TenantDocument doc : tenants) {
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
        final String podId = CamilleEnvironment.getPodId();
        final Camille camille = CamilleEnvironment.getCamille();
        Set<String> components = serviceService.getRegisteredServices();
        BootstrapState state = null;
        for (String serviceName : components) {
            Path tenantServiceStatePath = PathBuilder.buildCustomerSpaceServicePath(podId, contractId, tenantId,
                    CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, serviceName);
            BootstrapState newState = BootstrapState.createInitialState();
            try {
                if (camille.exists(tenantServiceStatePath)) {
                    newState = tenantEntityMgr.getTenantServiceState(contractId, tenantId, serviceName);
                }
            } catch (Exception e) {
                // ignore
            }
            if (newState != null) {
                // null means the tenant was provisioned without this component
                if (state == null) {
                    state = newState;
                } else if (!serviceName.equals(DanteComponent.componentName) || danteIsEnabled(contractId, tenantId)) {
                    state = mergeBootstrapStates(state, newState, serviceName);
                }
            }
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
        SerializableDocumentDirectory rawDir = tenantEntityMgr
                .getTenantServiceConfig(contractId, tenantId, serviceName);
        DocumentDirectory metaDir = serviceService.getConfigurationSchema(serviceName);
        rawDir.applyMetadataIgnoreOptionsValidation(metaDir);
        return rawDir;
    }

    @Override
    public SpaceConfiguration getDefaultSpaceConfig() {
        return tenantEntityMgr.getDefaultSpaceConfig();
    }

    @Override
    public DocumentDirectory getSpaceConfigSchema() {
        return batonService.getConfigurationSchema(spaceConfigNode);
    }

    @Override
    public boolean setupSpaceConfiguration(String contractId, String tenantId, SpaceConfiguration spaceConfig) {
        return batonService.setupSpaceConfiguration(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID,
                spaceConfig);
    }

    @Override
    public boolean danteIsEnabled(String contractId, String tenantId) {
        TenantDocument tenant = getTenant(contractId, tenantId);
        String str = tenant.getSpaceInfo().featureFlags;
        if (!str.contains(danteFeatureFlag)) {
            return false;
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode json = mapper.readTree(str);
            return json.get(danteFeatureFlag).asBoolean();
        } catch (Exception e) {
            return false;
        }
    }

    private static BootstrapState mergeBootstrapStates(BootstrapState state1, BootstrapState state2, String serviceName) {
        if (state1.state.equals(BootstrapState.State.ERROR) || state2.state.equals(BootstrapState.State.ERROR)) {
            return BootstrapState.constructErrorState(0, 0, "At least one of the components encountered an error : "
                    + serviceName);
        }

        if (state1.state.equals(BootstrapState.State.MIGRATED) || state2.state.equals(BootstrapState.State.MIGRATED)) {
            return BootstrapState.constructMigratedState();
        }

        if (state1.state.equals(BootstrapState.State.INITIAL) || state2.state.equals(BootstrapState.State.INITIAL)) {
            return BootstrapState.createInitialState();
        }

        return BootstrapState.constructOKState(state2.installedVersion);
    }
}
