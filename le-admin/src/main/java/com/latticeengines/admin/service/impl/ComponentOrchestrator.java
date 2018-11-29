package com.latticeengines.admin.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.admin.service.impl.TenantServiceImpl.ProductAndExternalAdminInfo;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapStateUtil;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.graph.traversal.impl.ReverseTopologicalTraverse;
import com.latticeengines.common.exposed.graph.traversal.impl.TopologicalTraverse;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.domain.exposed.component.ComponentStatus;
import com.latticeengines.domain.exposed.component.InstallDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.proxy.exposed.component.ComponentProxy;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

@Component
public class ComponentOrchestrator {

    @Autowired
    List<LatticeComponent> components;

    @Autowired
    private EmailService emailService;

    @Autowired
    private UserService userService;

    @Autowired
    private TenantService tenantService;

    @Value("${security.app.public.url:http://localhost:8081}")
    private String appPublicUrl;

    @Value("${common.pls.url}")
    private String plsEndHost;

    private static Map<String, LatticeComponent> componentMap;
    private static BatonService batonService = new BatonServiceImpl();
    private static ComponentProxy componentProxy = new ComponentProxy();

    private static final long TIMEOUT = 180000L;
    private static final long WAIT_INTERVAL = 1000L;
    private static final int NUM_RETRIES = (int) (TIMEOUT / WAIT_INTERVAL);

    private static final Logger log = LoggerFactory.getLogger(ComponentOrchestrator.class);

    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");
    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    public ComponentOrchestrator() {
    }

    public ComponentOrchestrator(List<LatticeComponent> components) {
        this.components = components;
        registerAll();
    }

    @PostConstruct
    public void postConstruct() {
        componentMap = new HashMap<>();
        for (LatticeComponent component : components) {
            componentMap.put(component.getName(), component);
        }
    }

    public List<LatticeComponent> getComponents() {
        return components;
    }

    public void registerAll() {
        Set<String> registered = batonService.getRegisteredServices();
        componentMap = new HashMap<>();
        for (LatticeComponent component : components) {
            if (!registered.contains(component.getName())) {
                component.register();
            }
            componentMap.put(component.getName(), component);
        }
    }

    public Set<String> getServiceNames() {
        return componentMap.keySet();
    }

    public Map<String, Set<LatticeProduct>> getServiceNamesWithProducts() {
        Map<String, Set<LatticeProduct>> serviceProductsMap = new HashMap<String, Set<LatticeProduct>>();
        Set<String> serviceNameSet = componentMap.keySet();
        for (String serviceName : serviceNameSet) {
            Set<LatticeProduct> products = componentMap.get(serviceName).getAssociatedProducts();
            serviceProductsMap.put(serviceName, products);
        }
        return serviceProductsMap;
    }

    public LatticeComponent getComponent(String serviceName) {
        if (componentMap.containsKey(serviceName)) {
            return componentMap.get(serviceName);
        } else {
            return null;
        }
    }

    public void orchestrateForInstall(String contractId, String tenantId, String spaceId,
            Map<String, Map<String, String>> properties, ProductAndExternalAdminInfo prodAndExternalAminInfo) {
        OrchestratorVisitor visitor = new OrchestratorVisitor(contractId, tenantId, spaceId, properties);
        TopologicalTraverse traverser = new TopologicalTraverse();
        traverser.traverse(components, visitor);

        postInstall(visitor, prodAndExternalAminInfo, tenantId);
    }

    public void orchestrateForInstallV2(String contractId, String tenantId, String spaceId,
                                      Map<String, Map<String, String>> properties, Map<String, String> tenantProperties,
                                      ProductAndExternalAdminInfo prodAndExternalAminInfo) {
        OrchestratorVisitorV2 visitor = new OrchestratorVisitorV2(contractId, tenantId, spaceId, properties, tenantProperties);
        TopologicalTraverse traverser = new TopologicalTraverse();
        traverser.traverse(components, visitor);

        postInstallV2(visitor, prodAndExternalAminInfo, tenantId);
    }

    public void orchestrateForUninstall(String contractId, String tenantId, String spaceId,
            Map<String, Map<String, String>> properties, ProductAndExternalAdminInfo prodAndExternalAminInfo,
            boolean deleteZookeeper) {
        OrchestratorVisitorForUninstall visitor = new OrchestratorVisitorForUninstall(contractId, tenantId, spaceId,
                properties);
        TopologicalTraverse traverser = new ReverseTopologicalTraverse();
        traverser.traverse(components, visitor);
        postUninstall(contractId, tenantId, deleteZookeeper);
    }

    private void postUninstall(String contractId, String tenantId, boolean deleteZookeeper) {
        if (deleteZookeeper) {
            boolean success = batonService.deleteContract(contractId);
            log.info(String.format("Deleting tenant %s with contract %s, success = %s", tenantId, contractId,
                    String.valueOf(success)));
        }
    }

    void postInstall(OrchestratorVisitor visitor, ProductAndExternalAdminInfo prodAndExternalAminInfo,
            String tenantId) {
        boolean installSuccess = checkComponentsBootStrapStatus(visitor);
        emailService(installSuccess, prodAndExternalAminInfo, tenantId);
    }

    void postInstallV2(OrchestratorVisitorV2 visitor, ProductAndExternalAdminInfo prodAndExternalAminInfo,
                     String tenantId) {
        boolean installSuccess = visitor.failed.size() == 0;
        emailService(installSuccess, prodAndExternalAminInfo, tenantId);
    }

    private boolean checkComponentsBootStrapStatus(OrchestratorVisitor visitor) {
        if (visitor.failed.size() == 0) {
            return true;
        }
        return false;
    }

    private void emailService(boolean allComponentsSuccessful, ProductAndExternalAdminInfo prodAndExternalAminInfo,
            String tenantId) {
        List<String> newEmailList = new ArrayList<String>();
        List<String> existingEmailList = new ArrayList<String>();
        List<LatticeProduct> products = prodAndExternalAminInfo.products;
        if (allComponentsSuccessful) {
            if (products.contains(LatticeProduct.PD)
                    || prodAndExternalAminInfo.products.contains(LatticeProduct.LPA3)) {
                Map<String, Boolean> externalEmailMap = prodAndExternalAminInfo.getExternalEmailMap();
                Set<String> externalEmails = externalEmailMap.keySet();
                for (String externalEmail : externalEmails) {
                    if (!externalEmailMap.get(externalEmail)) {
                        newEmailList.add(externalEmail);
                    } else {
                        existingEmailList.add(externalEmail);
                    }
                }
            }
        }
        sendExistingEmails(existingEmailList, tenantId, products);
        sendNewEmails(newEmailList, tenantId, products);
    }

    private void sendNewEmails(List<String> emailList, String tenantId, List<LatticeProduct> products) {
        for (String email : emailList) {
            User user = userService.findByEmail(email);
            if (user == null) {
                log.error(String.format("User: %s cannot be found", email));
                throw new RuntimeException(String.format("User: %s cannot be found", email));
            }
            // le-pls and le-admin uses the same encoding schema to be in synch
            log.info("The username is " + user.getUsername());

            // reset temp password so that user will have to change it when
            // login
            addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
            restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
            HttpHeaders headers = new HttpHeaders();
            headers.add("Content-Type", "application/json");
            headers.add("Accept", "application/json");
            HttpEntity<String> requestEntity = new HttpEntity<>(user.toString(), headers);

            ResponseEntity<String> tempPassword = restTemplate.exchange(plsEndHost + "/pls/admin/temppassword",
                    HttpMethod.PUT, requestEntity, String.class);

            if (products.equals(Collections.singletonList(LatticeProduct.PD))) {
                emailService.sendPdNewExternalUserEmail(user, tempPassword.getBody(), appPublicUrl);
            } else if (products.equals(Collections.singletonList(LatticeProduct.LPA3))) {
                emailService.sendPlsNewExternalUserEmail(user, tempPassword.getBody(), appPublicUrl, true);
                tenantService.updateTenantEmailFlag(tenantId, true);
            } else {
                log.info("The user clicked both PD and LPA3");
            }
        }
    }

    private void sendExistingEmails(List<String> emailList, String tenantId, List<LatticeProduct> products) {
        for (String email : emailList) {
            User user = userService.findByEmail(email);
            if (user == null) {
                log.error(String.format("User: %s cannot be found", email));
                throw new RuntimeException(String.format("User: %s cannot be found", email));
            }
            log.info("tenantId is " + tenantId);
            Tenant tenant = new Tenant();
            tenant.setName(tenantId);
            if (products.equals(Collections.singletonList(LatticeProduct.PD))) {
                emailService.sendPdExistingExternalUserEmail(tenant, user, appPublicUrl);
            } else if (products.equals(Collections.singletonList(LatticeProduct.LPA3))) {
                emailService.sendPlsExistingExternalUserEmail(tenant, user, appPublicUrl, true);
                tenantService.updateTenantEmailFlag(tenantId, true);
            } else {
                log.info("The user clicked both PD and LPA3");
            }
        }
    }

    private static class OrchestratorVisitor implements Visitor {
        public Set<String> failed = new HashSet<>();
        public final String contractId;
        public final String tenantId;
        public final String spaceId;
        public final Map<String, Map<String, String>> properties; // serviceName
                                                                  // ->
                                                                  // bootstrapProerties

        public OrchestratorVisitor(String contractId, String tenantId, String spaceId,
                Map<String, Map<String, String>> properties) {
            this.contractId = contractId;
            this.tenantId = tenantId;
            this.spaceId = spaceId;
            this.properties = properties;
        }

        @Override
        public void visit(Object o, VisitorContext ctx) {
            if (o instanceof LatticeComponent) {
                LatticeComponent component = (LatticeComponent) o;

                if (properties.containsKey(component.getName())) {

                    log.info(String.format("Attempt to install component %s for tenant %s", component.getName(),
                            tenantId));

                    List<? extends LatticeComponent> dependencies = component.getChildren();
                    for (LatticeComponent dependency : dependencies) {
                        if (this.failed.contains(dependency.getName())
                                || !(batonService.getTenantServiceBootstrapState(contractId, tenantId, spaceId,
                                        dependency.getName())).state.equals(BootstrapState.State.OK)) {
                            failed.add(component.getName());
                            String message = String.format("Component %s's dependency: %s is not met",
                                    component.getName(), dependency.getName());
                            log.error(message);
                            try {
                                BootstrapStateUtil.setState(
                                        PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                                                CustomerSpace.parse(tenantId), component.getName()),
                                        BootstrapState.constructErrorState(0, 0, message));
                            } catch (Exception e) {
                                log.error(String.format(
                                        "Unexpected failure at attempting to set bootstrap state for tenant %s component %s",
                                        tenantId, component.getName()));
                            }
                            return;
                        }
                    }

                    log.info("Dependencies for installing component " + component.getName() + " are all satisfied.");

                    Map<String, String> bootstrapProperties = properties.get(component.getName());
                    batonService.bootstrap(contractId, tenantId, spaceId, component.getName(), bootstrapProperties);

                    int numOfRetries = NUM_RETRIES;
                    BootstrapState state;
                    do {
                        state = batonService.getTenantServiceBootstrapState(contractId, tenantId, spaceId,
                                component.getName());
                        if (numOfRetries-- % 10 == 0) {
                            log.info(String.format(
                                    "Bootstrap status of [%s] of tenant %s is %s, %d out of %d retries remained.",
                                    component.getName(), tenantId, state.state.toString(), numOfRetries, NUM_RETRIES));
                        }
                        try {
                            Thread.sleep(WAIT_INTERVAL);
                        } catch (InterruptedException e) {
                            break;
                        }
                    } while (numOfRetries > 0 && state.state.equals(BootstrapState.State.INITIAL));

                    if (!state.state.equals(BootstrapState.State.OK)) {
                        failed.add(component.getName());
                    }

                }
            }
        }
    }

    private static class OrchestratorVisitorV2 implements Visitor {
        public Set<String> failed = new HashSet<>();
        public final String contractId;
        public final String tenantId;
        public final String spaceId;
        public final Map<String, Map<String, String>> properties; // serviceName
        public final Map<String, String> tenantProperties;
        // ->
        // bootstrapProerties

        public OrchestratorVisitorV2(String contractId, String tenantId, String spaceId,
                                     Map<String, Map<String, String>> properties, Map<String, String> tenantProperties) {
            this.contractId = contractId;
            this.tenantId = tenantId;
            this.spaceId = spaceId;
            this.properties = properties;
            this.tenantProperties = tenantProperties;
        }

        private InstallDocument generateInstallDoc(Map<String, String> props) {
            InstallDocument installDocument = new InstallDocument();
            installDocument.setDataVersion(1);
            installDocument.setVersionString("");
            if (MapUtils.isNotEmpty(props)) {
                for (Map.Entry<String, String> entry : props.entrySet()) {
                    if (entry.getKey().equalsIgnoreCase("/ExternalAdminEmails")) {
                        installDocument.addProperty(ComponentConstants.Install.EXTERNAL_ADMIN, entry.getValue());
                    } else if (entry.getKey().equalsIgnoreCase("/ThirdPartyUserEmails")) {
                        installDocument.addProperty(ComponentConstants.Install.THIRD_PARTY_USER, entry.getValue());
                    } else if (entry.getKey().equalsIgnoreCase("/LatticeAdminEmails")) {
                        installDocument.addProperty(ComponentConstants.Install.LATTICE_ADMIN, entry.getValue());
                    } else if (entry.getKey().equalsIgnoreCase("/SuperAdminEmails")) {
                        installDocument.addProperty(ComponentConstants.Install.SUPER_ADMIN, entry.getValue());
                    } else if (entry.getKey().equalsIgnoreCase("/EnrichAttributesMaxNumber")) {
                        installDocument.addProperty(ComponentConstants.Install.EA_MAX, entry.getValue());
                    }
                }
            }
            if (MapUtils.isNotEmpty(tenantProperties)) {
                tenantProperties.forEach(installDocument::addProperty);
            }
            return  installDocument;
        }

        @Override
        public void visit(Object o, VisitorContext ctx) {
            if (o instanceof LatticeComponent) {
                LatticeComponent component = (LatticeComponent) o;

                if (properties.containsKey(component.getName())) {

                    log.info(String.format("Attempt to install component %s for tenant %s", component.getName(),
                            tenantId));

                    List<? extends LatticeComponent> dependencies = component.getChildren();
                    for (LatticeComponent dependency : dependencies) {
                        if (dependency.hasV2Api()) {
                            if (this.failed.contains(dependency.getName()) || componentProxy.getComponentStatus
                                    (tenantId, dependency.getName()) != ComponentStatus.OK) {
                                failed.add(component.getName());
                                String message = String.format("Component %s's dependency: %s is not met",
                                        component.getName(), dependency.getName());
                                log.error(message);
                                if (component.hasV2Api()) {
                                    componentProxy.setComponentStatus(tenantId, component.getName(), ComponentStatus.ERROR);
                                } else {
                                    try {
                                        BootstrapStateUtil.setState(
                                                PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                                                        CustomerSpace.parse(tenantId), component.getName()),
                                                BootstrapState.constructErrorState(0, 0, message));
                                    } catch (Exception e) {
                                        log.error(String.format(
                                                "Unexpected failure at attempting to set bootstrap state for tenant %s component %s",
                                                tenantId, component.getName()));
                                    }
                                }
                                return;
                            }
                        } else {
                            if (this.failed.contains(dependency.getName())
                                    || !(batonService.getTenantServiceBootstrapState(contractId, tenantId, spaceId,
                                    dependency.getName())).state.equals(BootstrapState.State.OK)) {
                                failed.add(component.getName());
                                String message = String.format("Component %s's dependency: %s is not met",
                                        component.getName(), dependency.getName());
                                log.error(message);
                                try {
                                    BootstrapStateUtil.setState(
                                            PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                                                    CustomerSpace.parse(tenantId), component.getName()),
                                            BootstrapState.constructErrorState(0, 0, message));
                                } catch (Exception e) {
                                    log.error(String.format(
                                            "Unexpected failure at attempting to set bootstrap state for tenant %s component %s",
                                            tenantId, component.getName()));
                                }
                                return;
                            }
                        }
                    }

                    log.info("Dependencies for installing component " + component.getName() + " are all satisfied.");
                    Map<String, String> bootstrapProperties = properties.get(component.getName());
                    if (component.hasV2Api()) {
                        InstallDocument installDocument =  generateInstallDoc(bootstrapProperties);
                        componentProxy.install(tenantId, component.getName(), installDocument);
                        ComponentStatus status;
                        int numOfRetries = NUM_RETRIES;
                        do {
                            status = componentProxy.getComponentStatus(tenantId, component.getName());
                            if (numOfRetries-- % 10 == 0) {
                                log.info(String.format(
                                        "Bootstrap status of [%s] of tenant %s is %s, %d out of %d retries remained.",
                                        component.getName(), tenantId, status.name(), numOfRetries, NUM_RETRIES));
                            }
                            try {
                                Thread.sleep(WAIT_INTERVAL);
                            } catch (InterruptedException e) {
                                break;
                            }
                        } while (numOfRetries > 0 && ComponentStatus.INITIAL.equals(status));
                    } else {
                        batonService.bootstrap(contractId, tenantId, spaceId, component.getName(), bootstrapProperties);

                        int numOfRetries = NUM_RETRIES;
                        BootstrapState state;
                        do {
                            state = batonService.getTenantServiceBootstrapState(contractId, tenantId, spaceId,
                                    component.getName());
                            if (numOfRetries-- % 10 == 0) {
                                log.info(String.format(
                                        "Bootstrap status of [%s] of tenant %s is %s, %d out of %d retries remained.",
                                        component.getName(), tenantId, state.state.toString(), numOfRetries, NUM_RETRIES));
                            }
                            try {
                                Thread.sleep(WAIT_INTERVAL);
                            } catch (InterruptedException e) {
                                break;
                            }
                        } while (numOfRetries > 0 && state.state.equals(BootstrapState.State.INITIAL));

                        if (!state.state.equals(BootstrapState.State.OK)) {
                            failed.add(component.getName());
                        }
                    }

                }
            }
        }
    }

    private static class OrchestratorVisitorForUninstall extends OrchestratorVisitor {

        public OrchestratorVisitorForUninstall(String contractId, String tenantId, String spaceId,
                Map<String, Map<String, String>> properties) {
            super(contractId, tenantId, spaceId, properties);
        }

        @Override
        public void visit(Object o, VisitorContext ctx) {
            if (o instanceof LatticeComponent) {
                LatticeComponent component = (LatticeComponent) o;

                if (properties.containsKey(component.getName())) {

                    log.info(String.format("Attempt to uninstall component %s for tenant %s", component.getName(),
                            tenantId));

                    Map<String, String> bootstrapProperties = properties.get(component.getName());
                    batonService.bootstrap(contractId, tenantId, spaceId, component.getName(), bootstrapProperties);

                    int numOfRetries = NUM_RETRIES;
                    BootstrapState state;
                    do {
                        state = batonService.getTenantServiceBootstrapState(contractId, tenantId, spaceId,
                                component.getName());
                        if (numOfRetries-- % 10 == 0) {
                            log.info(String.format("Bootstrap status of [%s] is %s, %d out of %d retries remained.",
                                    component.getName(), state.state.toString(), numOfRetries, NUM_RETRIES));
                        }
                        try {
                            Thread.sleep(WAIT_INTERVAL);
                        } catch (InterruptedException e) {
                            break;
                        }
                    } while (numOfRetries > 0 && state.state.equals(BootstrapState.State.UNINSTALLING));

                    if (!state.state.equals(BootstrapState.State.UNINSTALLED)) {
                        failed.add(component.getName());
                    }

                }
            }
        }
    }
}
