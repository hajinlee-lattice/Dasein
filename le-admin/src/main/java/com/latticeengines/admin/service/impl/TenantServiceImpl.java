package com.latticeengines.admin.service.impl;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

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
import com.latticeengines.common.exposed.util.EmailUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory.Node;
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
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

@Component("adminTenantService")
public class TenantServiceImpl implements TenantService {
    private static final Log log = LogFactory.getLog(TenantServiceImpl.class);
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

    @Value("${pls.api.hostport}")
    private String plsEndHost;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private ProductAndExternalAdminInfo prodAndExternalAminInfo;

    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");

    protected RestTemplate restTemplate = new RestTemplate();

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

        preinstall(spaceConfig, configSDirs);

        // change components in orchestrator based on selected product
        // retrieve mappings from Camille
        final Map<String, Map<String, String>> orchestratorProps = props;
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                orchestrator.orchestrate(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID,
                        orchestratorProps, prodAndExternalAminInfo);
            }
        });

        return true;
    }

    protected void preinstall(SpaceConfiguration spaceConfig, List<SerializableDocumentDirectory> configDirectories) {
        // as a short term patch, waking up Dante's .NET App Pool is necessary
        // for it to pick up the bootstrap command.
        danteComponent.wakeUpAppPool();
        prodAndExternalAminInfo = getProductAndExternalAdminInfo(spaceConfig, configDirectories);

    }

    private ProductAndExternalAdminInfo getProductAndExternalAdminInfo(SpaceConfiguration spaceConfig,
            List<SerializableDocumentDirectory> configDirectories) {
        // Check if PD is external user existed
        if (configDirectories == null || spaceConfig == null) {
            return null;
        }
        ProductAndExternalAdminInfo prodAndExternalEmail = new ProductAndExternalAdminInfo();
        List<LatticeProduct> selectedProducts = spaceConfig.getProducts();
        List<String> externalEmailList = null;
        log.info(selectedProducts);
        for (SerializableDocumentDirectory configDirectory : configDirectories) {
            if (configDirectory.getRootPath().equals("/PLS")) {
                Collection<Node> nodes = configDirectory.getNodes();
                for (Node node : nodes) {
                    if (node.getNode().equals("ExternalAdminEmails")) {
                        String externalAminEmailsString = node.getData();
                        log.info(externalAminEmailsString);
                        externalEmailList = EmailUtils.parseEmails(externalAminEmailsString);
                    }
                }
            }
        }

        Map<String, Boolean> externalEmailMap = new HashMap<String, Boolean>();

        if (externalEmailList != null) {
            for (String email : externalEmailList) {
                externalEmailMap.put(email, checkExternalAdminUserExistence(email));
            }
        }
        prodAndExternalEmail.setProducts(selectedProducts);
        prodAndExternalEmail.setExternalEmailMap(externalEmailMap);

        return prodAndExternalEmail;
    }

    private boolean checkExternalAdminUserExistence(String externalEmail) {

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        URI attrUrl = UriComponentsBuilder.fromUriString(plsEndHost + "/pls/admin/users")
                .queryParam("useremail", externalEmail).build().toUri();
        log.info("Url Value " + attrUrl.toString());
        Boolean externalUserExists = restTemplate.getForObject(attrUrl, Boolean.class);
        return externalUserExists.booleanValue();
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
        TenantDocument doc = tenantEntityMgr.getTenant(contractId, tenantId);
        doc.setBootstrapState(getTenantOverallState(contractId, tenantId));
        return doc;
    }

    @Override
    public BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName) {
        return tenantEntityMgr.getTenantServiceState(contractId, tenantId, serviceName);
    }

    @Override
    public BootstrapState getTenantOverallState(String contractId, String tenantId) {
        final String podId = CamilleEnvironment.getPodId();
        final Camille camille = CamilleEnvironment.getCamille();

        TenantDocument doc = tenantEntityMgr.getTenant(contractId, tenantId);
        SpaceConfiguration spaceConfiguration = doc.getSpaceConfig();
        List<LatticeProduct> products = new ArrayList<>();
        if (spaceConfiguration != null) {
            products = spaceConfiguration.getProducts();
        }

        if (products.isEmpty()) {
            return BootstrapState.createInitialState();
        }

        Set<String> components = serviceService.getRegisteredServices();
        BootstrapState state = BootstrapState.createInitialState();
        for (String serviceName : components) {
            LatticeComponent latticeComponent = orchestrator.getComponent(serviceName);
            if (shouldHaveComponent(products, latticeComponent)) {
                Path tenantServiceStatePath = PathBuilder.buildCustomerSpaceServicePath(podId, contractId, tenantId,
                        CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, serviceName);
                BootstrapState newState;
                try {
                    if (camille.exists(tenantServiceStatePath)) {
                        newState = tenantEntityMgr.getTenantServiceState(contractId, tenantId, serviceName);
                    } else {
                        newState = BootstrapState.createInitialState();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Error getting the newState of the Service: %s", serviceName));
                }

                if (newState != null) {
                    if (state == null || state == BootstrapState.createInitialState()) {
                        state = newState;
                    } else
                        if (!serviceName.equals(DanteComponent.componentName) || danteIsEnabled(contractId, tenantId)) {
                        state = mergeBootstrapStates(state, newState, serviceName);
                    }
                }
            }
        }
        return state == null ? BootstrapState.createInitialState() : state;
    }

    @Override
    public boolean bootstrap(String contractId, String tenantId, String serviceName, Map<String, String> properties) {
        return batonService.bootstrap(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, serviceName,
                properties);
    }

    @Override
    public SerializableDocumentDirectory getTenantServiceConfig(String contractId, String tenantId,
            String serviceName) {
        SerializableDocumentDirectory rawDir = tenantEntityMgr.getTenantServiceConfig(contractId, tenantId,
                serviceName);
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
        TenantDocument tenant = tenantEntityMgr.getTenant(contractId, tenantId);
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

    private static BootstrapState mergeBootstrapStates(BootstrapState state1, BootstrapState state2,
            String serviceName) {
        if (state1.state.equals(BootstrapState.State.ERROR) || state2.state.equals(BootstrapState.State.ERROR)) {
            return BootstrapState.constructErrorState(0, 0,
                    "At least one of the components encountered an error : " + serviceName);
        }

        if (state1.state.equals(BootstrapState.State.MIGRATED) || state2.state.equals(BootstrapState.State.MIGRATED)) {
            return BootstrapState.constructMigratedState();
        }

        if (state1.state.equals(BootstrapState.State.INITIAL) || state2.state.equals(BootstrapState.State.INITIAL)) {
            return BootstrapState.createInitialState();
        }

        return BootstrapState.constructOKState(state2.installedVersion);
    }

    private boolean shouldHaveComponent(List<LatticeProduct> products, LatticeComponent latticeComponent) {
        Set<LatticeProduct> productsBelongTo = new HashSet<>(latticeComponent.getAssociatedProducts());
        productsBelongTo.retainAll(products);
        return !productsBelongTo.isEmpty();
    }

    public static class ProductAndExternalAdminInfo {
        public List<LatticeProduct> products;
        public Map<String, Boolean> externalEmailMap;

        public ProductAndExternalAdminInfo() {
        }

        public List<LatticeProduct> getProducts() {
            return this.products;
        }

        public void setProducts(List<LatticeProduct> products) {
            this.products = products;
        }

        public Map<String, Boolean> getExternalEmailMap() {
            return this.externalEmailMap;
        }

        public void setExternalEmailMap(Map<String, Boolean> externalEmailMap) {
            this.externalEmailMap = externalEmailMap;
        }
    }
}
