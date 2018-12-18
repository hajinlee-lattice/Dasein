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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.service.ServiceConfigService;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.DefaultConfigOverwriter;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.dante.DanteComponent;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapStateUtil;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.EmailUtils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory.Node;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapPropertyConstant;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

@Component("adminTenantService")
public class TenantServiceImpl implements TenantService {
    private static final Logger log = LoggerFactory.getLogger(TenantServiceImpl.class);
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
    private DefaultConfigOverwriter overwriter;

    @Autowired
    private ServiceConfigService serviceConfigService;

    @Autowired
    private com.latticeengines.security.exposed.service.TenantService tenantService;

    @Autowired
    private Oauth2RestApiProxy oauthProxy;

    @Value("${common.pls.url}")
    private String plsEndHost;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private ProductAndExternalAdminInfo prodAndExternalAminInfo;

    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");

    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

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
            overwriter.overwriteDefaultSpaceConfig();
        }

    }

    @Override
    public boolean createTenant(final String contractId, final String tenantId, TenantRegistration tenantRegistration,
            String userName) {
        final ContractInfo contractInfo = tenantRegistration.getContractInfo();
        final TenantInfo tenantInfo = tenantRegistration.getTenantInfo();
        final CustomerSpaceInfo spaceInfo = tenantRegistration.getSpaceInfo();
        final SpaceConfiguration spaceConfig = tenantRegistration.getSpaceConfig();

        try {
            if (TenantLifecycleManager.exists(contractId, tenantId)) {
                log.error(String.format("Error! tenant %s already exists in Zookeeper", tenantId));
                return false;
            }
        } catch (Exception e) {
            log.error("Error checking tenant", e);
        }

        tenantInfo.properties.userName = userName;
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

        boolean allowAutoSchedule = batonService.isEnabled(CustomerSpace.parse(tenantId),
                LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE);
        serviceConfigService.verifyInvokeTime(allowAutoSchedule, props);
        preinstall(spaceConfig, configSDirs);

        // change components in orchestrator based on selected product
        // retrieve mappings from Camille
        final Map<String, Map<String, String>> orchestratorProps = props;
        executorService.submit(() -> {
            orchestrator.orchestrateForInstall(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID,
                    orchestratorProps, prodAndExternalAminInfo);
            // record tenant status after being created
            TenantDocument tenantDoc = getTenant(contractId, tenantId);
            if (tenantDoc != null && !BootstrapState.State.OK.equals(tenantDoc.getBootstrapState().state)) {
                tenantInfo.properties.status = TenantStatus.INACTIVE.name();
                updateTenantInfo(contractId, tenantId, tenantInfo);
            }
        });
        return true;
    }

    @Override
    public boolean createTenantV2(String contractId, String tenantId, TenantRegistration tenantRegistration,
            String userName) {
        final ContractInfo contractInfo = tenantRegistration.getContractInfo();
        final TenantInfo tenantInfo = tenantRegistration.getTenantInfo();
        final CustomerSpaceInfo spaceInfo = tenantRegistration.getSpaceInfo();
        final SpaceConfiguration spaceConfig = tenantRegistration.getSpaceConfig();

        try {
            if (TenantLifecycleManager.exists(contractId, tenantId)) {
                log.error(String.format("Error! tenant %s already exists in Zookeeper", tenantId));
                return false;
            }
        } catch (Exception e) {
            log.error("Error checking tenant", e);
        }

        tenantInfo.properties.userName = userName;
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
        Map<String, String> tenantProps = getTenantProps(tenantInfo, spaceConfig);
        Map<String, Map<String, String>> props = new HashMap<>();
        for (SerializableDocumentDirectory configSDir : configSDirs) {
            String serviceName = configSDir.getRootPath().substring(1);
            Map<String, String> flatDir = configSDir.flatten();
            props.put(serviceName, flatDir);
        }

        boolean allowAutoSchedule = batonService.isEnabled(CustomerSpace.parse(tenantId),
                LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE);
        serviceConfigService.verifyInvokeTime(allowAutoSchedule, props);
        preinstall(spaceConfig, configSDirs);

        // change components in orchestrator based on selected product
        // retrieve mappings from Camille
        final Map<String, Map<String, String>> orchestratorProps = props;
        executorService.submit(() -> {
            orchestrator.orchestrateForInstallV2(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID,
                    orchestratorProps, tenantProps, prodAndExternalAminInfo);
            // record tenant status after being created
            // TenantDocument tenantDoc = getTenant(contractId, tenantId);
            // if (tenantDoc != null &&
            // !BootstrapState.State.OK.equals(tenantDoc.getBootstrapState().state))
            // {
            // tenantInfo.properties.status = TenantStatus.INACTIVE.name();
            // updateTenantInfo(contractId, tenantId, tenantInfo);
            // }
        });
        return true;
    }

    private Map<String, String> getTenantProps(TenantInfo tenantInfo, SpaceConfiguration spaceConfiguration) {
        Map<String, String> tenantProps = new HashMap<>();
        if (StringUtils.isNotEmpty(tenantInfo.properties.contract)) {
            tenantProps.put(ComponentConstants.Install.CONTRACT, tenantInfo.properties.contract);
        }
        if (StringUtils.isNotEmpty(tenantInfo.properties.status)) {
            tenantProps.put(ComponentConstants.Install.TENANT_STATUS, tenantInfo.properties.status);
        }
        if (StringUtils.isNotEmpty(tenantInfo.properties.tenantType)) {
            tenantProps.put(ComponentConstants.Install.TENANT_TYPE, tenantInfo.properties.tenantType);
        }
        if (StringUtils.isNotEmpty(tenantInfo.properties.userName)) {
            tenantProps.put(ComponentConstants.Install.USER_NAME, tenantInfo.properties.userName);
        }
        if (StringUtils.isNotEmpty(tenantInfo.properties.displayName)) {
            tenantProps.put(ComponentConstants.Install.TENANT_DISPLAY_NAME, tenantInfo.properties.displayName);
        }
        if (CollectionUtils.isNotEmpty(spaceConfiguration.getProducts())) {
            tenantProps.put(ComponentConstants.Install.PRODUCTS, JsonUtils.serialize(spaceConfiguration.getProducts()));
        }
        return tenantProps;
    }

    private void preinstall(SpaceConfiguration spaceConfig, List<SerializableDocumentDirectory> configDirectories) {
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
        log.info(StringUtils.join(", ", selectedProducts));
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
                doc.setBootstrapState(getTenantOverallState(cId, tId, doc));
            }
            return tenants;
        }
        return null;
    }

    @Override
    public Collection<TenantDocument> getTenantsInCache(String contractId) {
        PerformanceTimer timer = new PerformanceTimer("geting tenants in backend", log);
        Collection<TenantDocument> tenants = tenantEntityMgr.getTenantsInCache(contractId);
        if (tenants != null) {
            for (TenantDocument doc : tenants) {
                String cId = doc.getSpace().getContractId();
                String tId = doc.getSpace().getTenantId();
                doc.setBootstrapState(getTenantOverallState(cId, tId, doc));
            }
            timer.close();
            return tenants;
        }
        timer.close();
        return null;
    }

    @Override
    public boolean deleteTenant(final String userName, final String contractId, final String tenantId,
            final boolean deleteZookeeper) {
        CustomerSpace space = new CustomerSpace(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        if (getTenant(contractId, tenantId) == null) {
            log.info("Tenant " + tenantId + " seems not exist, nothing to delete.");
        }
        Map<String, Map<String, String>> props = new HashMap<>();
        for (LatticeComponent component : orchestrator.components) {
            BootstrapState state = getTenantServiceState(contractId, tenantId, component.getName());
            if (state.state.equals(BootstrapState.State.INITIAL) || state.state.equals(BootstrapState.State.UNINSTALLED)
                    || state.state.equals(BootstrapState.State.UNINSTALLING)) {
                continue;
            }
            SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(
                    batonService.getDefaultConfiguration(component.getName()));
            Map<String, String> properties = sDir.flatten();

            properties.put(BootstrapPropertyConstant.BOOTSTRAP_COMMAND, BootstrapPropertyConstant.BOOTSTRAP_UNINSTALL);
            Path path = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), space.getContractId(),
                    space.getTenantId(), space.getSpaceId(), component.getName());
            try {
                BootstrapStateUtil.setState(path, BootstrapState.constructDeletingState());
            } catch (Exception e) {
                log.error("Can't set bootstrap state to uninstalling!");
                if (deleteZookeeper) {
                    return tenantEntityMgr.deleteTenant(contractId, tenantId);
                } else {
                    return true;
                }
            }
            props.put(component.getName(), properties);
        }
        final Map<String, Map<String, String>> orchestratorProps = props;
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                orchestrator.orchestrateForUninstall(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID,
                        orchestratorProps, prodAndExternalAminInfo, deleteZookeeper);
            }
        });

        // delete oauth2 keys
        try {
            oauthProxy.deleteTenant(space.toString());
        } catch (Exception e) {
            log.info(String.format("No Oauth2 info related to this Tenant %s ", space.toString()));
        }
        log.info(String.format("Tenant %s deleted by user %s", tenantId, userName));
        return true;
    }

    @Override
    public TenantDocument getTenant(String contractId, String tenantId) {
        TenantDocument doc = tenantEntityMgr.getTenant(contractId, tenantId);
        if (doc == null) {
            return null;
        }
        doc.setBootstrapState(getTenantOverallState(contractId, tenantId, doc));
        return doc;
    }

    @Override
    public BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName) {
        return tenantEntityMgr.getTenantServiceState(contractId, tenantId, serviceName);
    }

    @Override
    public BootstrapState getTenantServiceStateInCache(String contractId, String tenantId, String serviceName) {
        return tenantEntityMgr.getTenantServiceStateInCache(contractId, tenantId, serviceName);
    }

    @Override
    public BootstrapState getTenantOverallState(String contractId, String tenantId, TenantDocument doc) {
        final String podId = CamilleEnvironment.getPodId();
        final Camille camille = CamilleEnvironment.getCamille();

        SpaceConfiguration spaceConfiguration = doc.getSpaceConfig();
        List<LatticeProduct> products = new ArrayList<>();
        if (spaceConfiguration != null) {
            products = spaceConfiguration.getProducts();
        }

        if (products.isEmpty()) {
            return BootstrapState.createInitialState();
        }

        Set<String> components = serviceService.getRegisteredServices();
        log.info(String.format("Checking status of services %s for tenant %s", components, tenantId));
        BootstrapState state = null;
        for (String serviceName : components) {
            LatticeComponent latticeComponent = orchestrator.getComponent(serviceName);
            if (shouldHaveComponent(products, latticeComponent)) {
                Path tenantServiceStatePath = PathBuilder.buildCustomerSpaceServicePath(podId, contractId, tenantId,
                        CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, serviceName);
                BootstrapState newState;
                try {
                    if (camille.exists(tenantServiceStatePath)) {
                        newState = tenantEntityMgr.getTenantServiceStateInCache(contractId, tenantId, serviceName);
                    } else {
                        newState = BootstrapState.createInitialState();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(String.format(
                            "Error getting the newState of the Service: %s for tenant %s", serviceName, tenantId));
                }
                log.info(String.format("State of required service %s for tenant %s is %s", serviceName, tenantId,
                        newState));

                if (newState != null) {
                    if (state == null) {
                        state = newState;
                    } else if (!serviceName.equals(DanteComponent.componentName) || danteIsEnabled(doc)) {
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
        rawDir = serviceConfigService.setDefaultInvokeTime(serviceName, rawDir);
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

    public boolean updateTenantInfo(final String contractId, final String tenantId, TenantInfo tenantInfo) {
        Camille camille = CamilleEnvironment.getCamille();
        Path tenantPath = PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId);
        Document properties = DocumentUtils.toRawDocument(tenantInfo.properties);
        Path propertiesPath = tenantPath.append(PathConstants.PROPERTIES_FILE);

        // updating data in zookeeper
        try {
            camille.upsert(propertiesPath, properties, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (Exception e) {
            log.debug("error during updating properties @ {}", propertiesPath);
            return false;
        }

        // updating data in DB
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            return false;
        }
        try {
            if (StringUtils.isNotBlank(tenantInfo.properties.status)) {
                tenant.setStatus(TenantStatus.valueOf(tenantInfo.properties.status));
            }
            if (StringUtils.isNotBlank(tenantInfo.properties.tenantType)) {
                tenant.setTenantType(TenantType.valueOf(tenantInfo.properties.tenantType));
            }
            tenant.setContract(tenantInfo.properties.contract);
            tenantService.updateTenant(tenant);
        } catch (Exception e) {
            log.debug(String.format("Failed to retrieve tenants properties or update tenant %s error.", tenantId));
            return false;
        }
        return true;
    }

    private boolean danteIsEnabled(TenantDocument tenant) {
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
