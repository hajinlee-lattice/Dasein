package com.latticeengines.admin.service.impl;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.service.FeatureFlagService;
import com.latticeengines.admin.service.ServiceConfigService;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.service.VboRequestLogService;
import com.latticeengines.admin.tenant.batonadapter.DefaultConfigOverwriter;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.cdl.CDLComponent;
import com.latticeengines.admin.tenant.batonadapter.dante.DanteComponent;
import com.latticeengines.admin.tenant.batonadapter.datacloud.DataCloudComponent;
import com.latticeengines.admin.tenant.batonadapter.dcp.DCPComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
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
import com.latticeengines.domain.exposed.admin.LatticeModule;
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
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.dcp.idaas.IDaaSUser;
import com.latticeengines.domain.exposed.dcp.vbo.VboCallback;
import com.latticeengines.domain.exposed.dcp.vbo.VboRequest;
import com.latticeengines.domain.exposed.dcp.vbo.VboResponse;
import com.latticeengines.domain.exposed.dcp.vbo.VboStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.monitor.tracing.TracingTags;
import com.latticeengines.monitor.util.TracingUtils;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.service.IDaaSService;
import com.latticeengines.security.service.VboService;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

@Component("adminTenantService")
public class TenantServiceImpl implements TenantService {
    private static final Logger log = LoggerFactory.getLogger(TenantServiceImpl.class);
    private static final String spaceConfigNode = LatticeComponent.spaceConfigNode;
    private static final String danteFeatureFlag = "Dante";

    private final BatonService batonService = new BatonServiceImpl();

    @Value("${admin.default.subscription.number}")
    private String DEFAULT_SUBSCRIPTION_NUMBER;

    @Inject
    private DanteComponent danteComponent;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private ServiceService serviceService;

    @Inject
    private IDaaSService iDaaSService;

    @Inject
    private VboService vboService;

    @Inject
    private VboRequestLogService vboRequestLogService;

    @Inject
    private ComponentOrchestrator orchestrator;

    @Inject
    private DefaultConfigOverwriter overwriter;

    @Inject
    private ServiceConfigService serviceConfigService;

    @Inject
    private com.latticeengines.security.exposed.service.TenantService tenantService;

    @Inject
    private Oauth2RestApiProxy oauthProxy;

    @Inject
    private MatchProxy matchProxy;

    @Inject
    private FeatureFlagService featureFlagService;

    @Inject
    private UserService userService;

    @Inject
    private EmailService emailService;

    @Value("${common.pls.url}")
    private String plsEndHost;

    @Value("${common.dcp.public.url}")
    private String dcpPublicUrl;

    @Value("${admin.vbo.callback.url}")
    private String callbackUrl;

    @Value("${admin.vbo.callback.canmock}")
    private boolean canMock;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private ProductAndExternalAdminInfo prodAndExternalAminInfo;

    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");

    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    public TenantServiceImpl() {
    }

    @PostConstruct
    protected void uploadDefaultSpaceConfigAndSchemaByJson() {
        boolean needToRegister = Boolean.parseBoolean(System.getProperty("com.latticeengines.registerBootstrappers"));
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
            String userName, VboCallback callback, String traceId) {
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

        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        boolean allowAutoSchedule = batonService.isEnabled(customerSpace,
                LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE);
        serviceConfigService.verifyInvokeTime(allowAutoSchedule, props);
        preinstall(spaceConfig, configSDirs);

        // change components in orchestrator based on selected product
        // retrieve mappings from Camille
        final Map<String, Map<String, String>> orchestratorProps = props;
        executorService.submit(() -> {
            boolean isDnBConnectRequest = batonService.hasProduct(customerSpace, LatticeProduct.DCP);
            CallbackTimeoutThread timeoutThread = null;
            Semaphore timeoutSemaphore = new Semaphore(1); // only to be used when updating timeout status
            if (callback != null) {
                timeoutThread = new CallbackTimeoutThread(callback, traceId, timeoutSemaphore);
                timeoutThread.start();
            }

            orchestrator.orchestrateForInstall(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID,
                    orchestratorProps, prodAndExternalAminInfo, isDnBConnectRequest, callback);

            // record tenant status after being created
            TenantDocument tenantDoc = getTenant(contractId, tenantId);
            if (tenantDoc != null) {
                if (!BootstrapState.State.OK.equals(tenantDoc.getBootstrapState().state)) {
                    tenantInfo.properties.status = TenantStatus.INACTIVE.name();
                }
                updateTenantInfo(contractId, tenantId, tenantInfo);
            }

            if (callback != null) {
                try {
                    timeoutSemaphore.acquire();
                    if (!callback.timeout) {
                        callback.timeout = true;
                        timeoutThread.interrupt();
                        timeoutSemaphore.release();

                        vboRequestLogService.updateVboCallback(traceId, callback, System.currentTimeMillis());

                        vboService.sendProvisioningCallback(callback);
                    } else {
                        timeoutSemaphore.release();
                        // callback timed out: should we clean up tenant?
                        // deleteTenant(userName, contractId, tenantId, true);
                    }
                } catch (InterruptedException e) {
                    log.error("Unexpected semaphore interruption");
                    callback.customerCreation.transactionDetail.status = VboStatus.OTHER;
                }
            }
        });

        // make sure no match entries leftover
        if (batonService.isEntityMatchEnabled(customerSpace)) {
            BumpVersionRequest request = new BumpVersionRequest();
            request.setTenant(new Tenant(tenantId));
            request.setEnvironments(Arrays.asList(EntityMatchEnvironment.SERVING, EntityMatchEnvironment.STAGING));
            BumpVersionResponse response = matchProxy.bumpVersion(request);
            log.info(
                    "Entity match is enabled for tenant {}, bumping up version to have a clean state. Current version = {}",
                    tenantId, response.getVersions());
        }

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

        Map<String, Boolean> externalEmailMap = new HashMap<>();

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
        return Boolean.TRUE.equals(externalUserExists);
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
        PerformanceTimer timer = new PerformanceTimer("getting tenants in backend", log);
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
        executorService.submit(() -> orchestrator.orchestrateForUninstall(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID,
                orchestratorProps, prodAndExternalAminInfo, deleteZookeeper));
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
        doc.setFeatureFlags(overlayDefaultValues(doc.getFeatureFlags(), doc.getSpaceConfig().getProducts()));
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

    @Override
    public List<LatticeModule> updateModules(String contractId, String tenantId, Collection<LatticeModule> modules) {
        TenantDocument tenantDocument = getTenant(contractId, tenantId);
        if (tenantDocument == null) {
            throw new IllegalArgumentException("Tenant not exist: contractId=" + contractId + ", tenantId=" + tenantId);
        }
        SpaceConfiguration spaceConfiguration = tenantDocument.getSpaceConfig();
        if (spaceConfiguration == null) {
            spaceConfiguration = getDefaultSpaceConfig();
        }
        spaceConfiguration.setModules(new ArrayList<>(modules));
        setupSpaceConfiguration(contractId, tenantId, spaceConfiguration);
        tenantDocument = getTenant(contractId, tenantId);
        return tenantDocument.getSpaceConfig().getModules();
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
            if (tenantInfo.properties.expiredTime != null) {
                tenant.setExpiredTime(tenantInfo.properties.expiredTime);
            }
            tenant.setContract(tenantInfo.properties.contract);
            tenantService.updateTenant(tenant);
        } catch (Exception e) {
            log.debug(String.format("Failed to retrieve tenants properties or update tenant %s error.", tenantId));
            return false;
        }
        return true;
    }

    @Override
    public VboResponse createVboTenant(VboRequest vboRequest, String userName, String requestUrl, Boolean callback, Boolean useMock) {
        Long receiveTime = System.currentTimeMillis();
        String subNumber  = vboRequest.getSubscriber().getSubscriberNumber();
        Tenant existingTenant = tenantService.findBySubscriberNumber(subNumber);
        if (existingTenant != null) {
            String existingName = existingTenant.getName();
            log.error("the subscriber number {} has been registered by tenant {}",
                    subNumber, existingName);
            VboResponse response = generateVBOResponse("failed",
                    "A tenant already exists for this subscriber number.");
            vboRequestLogService.createVboRequestLog(null, null, receiveTime, vboRequest, response);
            return response;
        }

        // Construct tenant name from subscriber name
        String subName = vboRequest.getSubscriber().getName();
        String tenantName = constructTenantNameFromSubscriber(subName);
        if (StringUtils.isBlank(tenantName)) {
            log.error("system can't construct tenant name from subscriber name {}.", subName);
            VboResponse response = generateVBOResponse("failed",
                    "system can't construct tenant name from subscriber name.");
            vboRequestLogService.createVboRequestLog(null, null, receiveTime, vboRequest, response);
            return response;
        }

        // If a tenantType == TenantType.CUSTOMER tenant (the default) then check that the subscriber number is valid.
        TenantType subscriberTenantType = vboRequest.getSubscriber().getTenantType();
        boolean validSubscriberNumber = iDaaSService.doesSubscriberNumberExist(vboRequest);
        if (subscriberTenantType == TenantType.CUSTOMER && !validSubscriberNumber) {
            String msg = String.format("The subscriber number [%s] is not valid, unable to create tenant.", subNumber);
            log.error(msg);
            VboResponse response = generateVBOResponse("failed", msg);
            vboRequestLogService.createVboRequestLog(null, null, receiveTime, vboRequest, response);
            return response;
        }
        else if (subscriberTenantType != TenantType.CUSTOMER && !validSubscriberNumber) {
            // If not a customer TenantType and subscriber number is invalid then use the default subscription number.
            log.info("The subscriber number {} is not valid, using the default subscriber number for tenant {}", subNumber, tenantName);
            subNumber = DEFAULT_SUBSCRIPTION_NUMBER;
        }

        Tracer tracer = GlobalTracer.get();
        Span adminSpan = null;
        long start = System.currentTimeMillis() * 1000;
        try (Scope scope = startAdminSpan(tenantName, start)) {
            adminSpan = tracer.activeSpan();
            Map<String, String> tracingCtx = TracingUtils.getActiveTracingContext();
            String traceId = adminSpan.context().toTraceId();
            vboRequestLogService.createVboRequestLog(traceId, tenantName, receiveTime, vboRequest, null);

            List<LatticeProduct> productList = Arrays.asList(LatticeProduct.LPA3, LatticeProduct.CG, LatticeProduct.DCP);

            // TenantInfo
            TenantProperties tenantProperties = new TenantProperties();
            tenantProperties.description = "A tenant created by vbo request";
            tenantProperties.displayName = tenantName;
            TenantInfo tenantInfo = new TenantInfo(tenantProperties);

            // FeatureFlags
            FeatureFlagDefinitionMap definitionMap = featureFlagService.getDefinitions();
            FeatureFlagValueMap defaultValueMap = new FeatureFlagValueMap();
            definitionMap.forEach((flagId, flagDef) -> {
                // Do not fix D&B Connect-specific flags to creation-time default; let it sync with current default
                // Others should be fixed at this point
                boolean fixNow = flagDef.getAvailableProducts() == null ||
                                    (!flagDef.getAvailableProducts().equals(Collections.singleton(LatticeProduct.DCP)) &&
                                        productList.stream().anyMatch(product -> flagDef.getAvailableProducts().contains(product)));
                if (fixNow) {
                    boolean defaultVal = flagDef.getDefaultValue();
                    defaultValueMap.put(flagId, defaultVal);
                }
            });

            // SpaceInfo
            CustomerSpaceProperties spaceProperties = new CustomerSpaceProperties();
            spaceProperties.description = tenantProperties.description;
            spaceProperties.displayName = tenantProperties.displayName;

            CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(spaceProperties, JsonUtils.serialize(defaultValueMap));

            // SpaceConfiguration
            SpaceConfiguration spaceConfiguration = getDefaultSpaceConfig();
            spaceConfiguration.setProducts(productList);

            List<String> services = Arrays.asList(PLSComponent.componentName, CDLComponent.componentName, DataCloudComponent.componentName, DCPComponent.componentName);

            List<SerializableDocumentDirectory> configDirs = new ArrayList<>();

            // generate email list to be added and IDaaS user list
            List<IDaaSUser> users = new ArrayList<>();
            for (VboRequest.User user : vboRequest.getProduct().getUsers()) {
                users.add(constructIDaaSUser(user, vboRequest.getSubscriber()));
            }

            for (String component : services) {
                SerializableDocumentDirectory componentConfig = serviceService.getDefaultServiceConfig(component);
                if (component.equalsIgnoreCase(PLSComponent.componentName)) {
                    // add users node
                    if (CollectionUtils.isNotEmpty(users)) {
                        SerializableDocumentDirectory.Node node = new SerializableDocumentDirectory.Node();
                        node.setNode("IDaaSUsers");
                        node.setData(JsonUtils.serialize(users));
                        componentConfig.getNodes().add(node);
                    }
                    // add subscriberNum node
                    SerializableDocumentDirectory.Node node = new SerializableDocumentDirectory.Node();
                    node.setNode("SubscriberNumber");
                    node.setData(subNumber);
                    componentConfig.getNodes().add(node);
                    // set null as document dir and serializable document dir are not consistent, document directory will
                    // be newly constructed from serializable document dir when needed
                    componentConfig.setDocumentDirectory(null);

                }
                if (MapUtils.isNotEmpty(tracingCtx)) {
                    SerializableDocumentDirectory.Node node = new SerializableDocumentDirectory.Node();
                    node.setNode("TracingContext");
                    node.setData(JsonUtils.serialize(tracingCtx));
                    if (componentConfig.getNodes() == null) {
                        componentConfig.setNodes(new ArrayList<>());
                    }
                    componentConfig.getNodes().add(node);
                    componentConfig.setRootPath("/" + component);
                    componentConfig.setDocumentDirectory(null);
                } else {
                    log.error("Cannot get current active tracing context.");
                }
                configDirs.add(componentConfig);
            }

            TenantRegistration registration = new TenantRegistration();
            registration.setContractInfo(new ContractInfo(new ContractProperties()));
            registration.setSpaceConfig(spaceConfiguration);
            registration.setSpaceInfo(spaceInfo);
            registration.setTenantInfo(tenantInfo);
            registration.setConfigDirectories(configDirs);

            VboCallback vboCallback = null;
            if (callback) {
                log.info("Generating callback for call to " + requestUrl);

                vboCallback = new VboCallback();
                vboCallback.customerCreation = new VboCallback.CustomerCreation();
                vboCallback.customerCreation.transactionDetail = new VboCallback.TransactionDetail();
                vboCallback.customerCreation.customerDetail = new VboCallback.CustomerDetail();

                vboCallback.customerCreation.transactionDetail.ackRefId = traceId;
                vboCallback.customerCreation.customerDetail.workspaceCountry = vboRequest.getSubscriber().getCountryCode();
                vboCallback.customerCreation.customerDetail.subscriberNumber = vboRequest.getSubscriber().getSubscriberNumber();

                // TODO: should this use getUserId() or getEmailAddress()? Related: ComponentOrchestrator::finalizeDnBConnect
                vboCallback.customerCreation.customerDetail.login = vboRequest.getProduct().getUsers().stream().findFirst().get().getEmailAddress();

                // callback needs current status if timeout triggered: assume failed unless proven otherwise
                vboCallback.customerCreation.transactionDetail.status = VboStatus.ALL_FAIL;
                vboCallback.customerCreation.customerDetail.emailSent = Boolean.toString(false);

                vboCallback.timeout = Boolean.FALSE;
                vboCallback.targetUrl = (canMock && useMock) ? ("https" + requestUrl.substring(requestUrl.indexOf(':'), requestUrl.indexOf("/tenants")) + "/vbomock") : callbackUrl;
            }

            boolean result = createTenant(tenantName, tenantName, registration, userName, vboCallback, traceId);
            String status = result ? "success" : "failed";
            String message = result ? "tenant created successfully via Vbo request" :
                    "tenant created failed via Vbo request";
            log.info("create tenant {} from vbo request", tenantName);
            VboResponse response = generateVBOResponse(status, message, traceId);
            vboRequestLogService.updateVboResponse(traceId, response);
            return response;
        } finally {
            TracingUtils.finish(adminSpan);
        }
    }

    private Scope startAdminSpan(String tenantName, long startTimeStamp) {
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("Bootstrap Tenant - " + tenantName)
                .withTag(TracingTags.TENANT_ID, tenantName)
                .withStartTimestamp(startTimeStamp)
                .start();
        return tracer.activateSpan(span);
    }

    private IDaaSUser constructIDaaSUser(VboRequest.User user, VboRequest.Subscriber subscriber) {
        IDaaSUser iDaasuser = new IDaaSUser();
        iDaasuser.setFirstName(user.getName().getFirstName());
        iDaasuser.setEmailAddress(user.getEmailAddress());
        iDaasuser.setLastName(user.getName().getLastName());
        iDaasuser.setUserName(user.getUserId());
        iDaasuser.setPhoneNumber(user.getTelephoneNumber());
        iDaasuser.setLanguage(subscriber.getLanguage());
        iDaasuser.setSubscriberNumber(subscriber.getSubscriberNumber());
        iDaasuser.setCountryCode(user.getPrimaryAddress().getCountryISOAlpha2Code());
        iDaasuser.setCompanyName(subscriber.getName());
        Preconditions.checkState(StringUtils.isNotEmpty(iDaasuser.getLastName()),
                "Last name is required");
        Preconditions.checkState(StringUtils.isNotEmpty(iDaasuser.getEmailAddress()),
                "Email is required");
        Preconditions.checkState(StringUtils.isNotEmpty(iDaasuser.getUserName()),
                "User name is required");
        Preconditions.checkState(StringUtils.isNotEmpty(iDaasuser.getLanguage()),
                "Language is required");
        Preconditions.checkState(StringUtils.isNotEmpty(iDaasuser.getPhoneNumber()),
                "Phone number is required");
        return iDaasuser;
    }

    private String constructTenantNameFromSubscriber(String subscriberName) {
        if (StringUtils.isBlank(subscriberName)) {
            log.info("empty subscriber name");
            return null;
        }
        String tenantName = subscriberName
                .trim()
                .replace(" ", "_")
                .replace(".", "")
                .replace(",", "");
        String encodedName;
        try {
            encodedName = URLEncoder.encode(tenantName, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            encodedName = null;
        }
        // compare the tenant name with encoded value, skip installation process if value is different now
        if (!tenantName.equals(encodedName)) {
            log.info("tenant name {} is not equal to encoded name {}", tenantName, encodedName);
            return null;
        }
        int suffix = 1;
        String uniqueTenantName = tenantName;
        String tenantId = CustomerSpace.parse(uniqueTenantName).toString();
        while (tenantService.hasTenantId(tenantId)) {
            uniqueTenantName = String.format("%s_%s", tenantName, suffix++);
            tenantId = CustomerSpace.parse(uniqueTenantName).toString();
        }
        return uniqueTenantName;
    }

    private VboResponse generateVBOResponse(String status, String message) {
        return generateVBOResponse(status, message, null);
    }

    private VboResponse generateVBOResponse(String status, String message, String traceId) {
        VboResponse vboResponse = new VboResponse();
        vboResponse.setStatus(status);
        vboResponse.setMessage(message);
        if (StringUtils.isNotEmpty(traceId)) {
            vboResponse.setAckReferenceId(traceId);
        }
        return vboResponse;
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

    private FeatureFlagValueMap overlayDefaultValues(FeatureFlagValueMap flagValueMap, List<LatticeProduct> products) {
        FeatureFlagDefinitionMap definitionMap = featureFlagService.getDefinitions();
        FeatureFlagValueMap newValueMap = new FeatureFlagValueMap();
        definitionMap.forEach((flagId, flagDef) -> {
            if (CollectionUtils.isNotEmpty(products) && CollectionUtils.isNotEmpty(flagDef.getAvailableProducts())
                    && !Collections.disjoint(products, flagDef.getAvailableProducts())) {
                boolean defaultVal = flagDef.getDefaultValue();
                newValueMap.put(flagId, defaultVal);
            }
        });
        if (flagValueMap != null) {
            newValueMap.putAll(flagValueMap);
        }
        return newValueMap;
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

    private class CallbackTimeoutThread extends Thread {
        private final VboCallback callback;
        private final String traceId;
        private final Semaphore semaphore;

        private final long WAIT_TIME = TimeUnit.MINUTES.toMillis(30);

        CallbackTimeoutThread(final VboCallback callback, final String traceId, Semaphore semaphore) {
            this.callback = callback;
            this.traceId = traceId;
            this.semaphore = semaphore;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(WAIT_TIME);
                semaphore.acquire();
                if (!callback.timeout) {
                    callback.timeout = true;
                    log.info("Callback timed out: sending callback as-is");
                    vboService.sendProvisioningCallback(callback);

                    vboRequestLogService.updateVboCallback(traceId, callback, System.currentTimeMillis());
                }
                semaphore.release();
            } catch (InterruptedException e) {
                // expected interrupt if main thread already performed callback: absorb
                log.info("Stopping callback timer");
            }
        }
    }
}
