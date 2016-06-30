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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.admin.service.impl.TenantServiceImpl.ProductAndAdminInfo;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.common.exposed.graph.traversal.impl.TopologicalTraverse;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.service.EmailService;
import com.latticeengines.security.exposed.service.UserService;

@Component
public class ComponentOrchestrator {

    @Autowired
    List<LatticeComponent> components;

    @Autowired
    private EmailService emailService;

    @Autowired
    private UserService userService;

    @Value("${security.app.public.url:http://localhost:8081}")
    private String appPublicUrl;

    @Value("${pls.api.hostport}")
    private String plsEndHost;

    private static Map<String, LatticeComponent> componentMap;
    private static BatonService batonService = new BatonServiceImpl();

    private static final long TIMEOUT = 180000L;
    private static final long WAIT_INTERVAL = 1000L;
    private static final int NUM_RETRIES = (int) (TIMEOUT / WAIT_INTERVAL);

    private static final Log log = LogFactory.getLog(ComponentOrchestrator.class);

    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");
    protected RestTemplate restTemplate = new RestTemplate();

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

    public void orchestrate(String contractId, String tenantId, String spaceId,
            Map<String, Map<String, String>> properties, ProductAndAdminInfo prodAndAminInfo) {
        OrchestratorVisitor visitor = new OrchestratorVisitor(contractId, tenantId, spaceId, properties);
        TopologicalTraverse traverser = new TopologicalTraverse();
        traverser.traverse(components, visitor);

        postInstall(visitor, prodAndAminInfo, tenantId);
    }

    void postInstall(OrchestratorVisitor visitor, ProductAndAdminInfo prodAndAminInfo, String tenantId) {
        boolean installSuccess = checkComponentsBootStrapStatus(visitor);
        emailService(installSuccess, prodAndAminInfo, tenantId);
    }

    private boolean checkComponentsBootStrapStatus(OrchestratorVisitor visitor) {
        if (visitor.failed.size() == 0) {
            return true;
        }
        return false;
    }

    private void emailService(boolean allComponentsSuccessful, ProductAndAdminInfo prodAndAminInfo, String tenantId) {
        List<String> newExternalEmailList = new ArrayList<String>();
        List<String> existingExternalEmailList = new ArrayList<String>();
        List<String> newInternalEmailList = new ArrayList<String>();
        List<String> existingInternalEmailList = new ArrayList<String>();

        List<LatticeProduct> products = prodAndAminInfo.products;
        if (allComponentsSuccessful) {
            if (products.contains(LatticeProduct.PD) || prodAndAminInfo.products.contains(LatticeProduct.LPA3)) {
                Map<String, Boolean> externalAdminEmailMap = prodAndAminInfo.getExternalAdminEmailMap();
                Set<String> externaldminEmails = externalAdminEmailMap.keySet();
                for (String adminEmail : externaldminEmails) {
                    if (!externalAdminEmailMap.get(adminEmail)) {
                        newExternalEmailList.add(adminEmail);
                    } else {
                        existingExternalEmailList.add(adminEmail);
                    }
                }

                Map<String, Boolean> internalAdminEmailMap = prodAndAminInfo.getInternalAdminEmailMap();
                Set<String> internalAdminEmails = internalAdminEmailMap.keySet();
                for (String adminEmail : internalAdminEmails) {
                    if (!internalAdminEmailMap.get(adminEmail)) {
                        newInternalEmailList.add(adminEmail);
                    } else {
                        existingInternalEmailList.add(adminEmail);
                    }
                }
            }
        }
        sendExistingExternalEmails(existingExternalEmailList, tenantId, products);
        sendNewExternalEmails(newExternalEmailList, products);
        sendExistingInternalEmails(existingInternalEmailList, tenantId, products);
        sendNewInternalEmails(newInternalEmailList, products, tenantId);
    }

    private void sendNewInternalEmails(List<String> newInternalEmailList, List<LatticeProduct> products, String tenantId) {
        for (String email : newInternalEmailList) {
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

            log.info("tenantId is " + tenantId);
            Tenant tenant = new Tenant();
            tenant.setName(tenantId);
            if (products.equals(Collections.singletonList(LatticeProduct.PD))) {
                emailService.sendPdNewInternalUserEmail(user, tempPassword.getBody(), appPublicUrl);
            } else if (products.equals(Collections.singletonList(LatticeProduct.LPA3))) {
                emailService.sendPlsNewInternalUserEmail(tenant, user, tempPassword.getBody(), appPublicUrl);
            } else {
                log.info("The user clicked both PD and LPA3");
            }
        }

    }

    private void sendExistingInternalEmails(List<String> existingInternalEmailList, String tenantId,
            List<LatticeProduct> products) {
        for (String email : existingInternalEmailList) {
            User user = userService.findByEmail(email);
            if (user == null) {
                log.error(String.format("User: %s cannot be found", email));
                throw new RuntimeException(String.format("User: %s cannot be found", email));
            }
            log.info("tenantId is " + tenantId);
            Tenant tenant = new Tenant();
            tenant.setName(tenantId);
            if (products.equals(Collections.singletonList(LatticeProduct.PD))) {
                emailService.sendPdExistingInternalUserEmail(tenant, user, appPublicUrl);
            } else if (products.equals(Collections.singletonList(LatticeProduct.LPA3))) {
                emailService.sendPlsExistingInternalUserEmail(tenant, user, appPublicUrl);
            } else {
                log.info("The user clicked both PD and LPA3");
            }
        }
    }

    private void sendNewExternalEmails(List<String> emailList, List<LatticeProduct> products) {
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
                emailService.sendPlsNewExternalUserEmail(user, tempPassword.getBody(), appPublicUrl);
            } else {
                log.info("The user clicked both PD and LPA3");
            }
        }
    }

    private void sendExistingExternalEmails(List<String> emailList, String tenantId, List<LatticeProduct> products) {
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
                emailService.sendPlsExistingExternalUserEmail(tenant, user, appPublicUrl);
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

                    log.info("Attempt to install component " + component.getName());

                    List<? extends LatticeComponent> dependencies = component.getChildren();
                    for (LatticeComponent dependency : dependencies) {
                        if (this.failed.contains(dependency.getName())
                                || !(batonService.getTenantServiceBootstrapState(contractId, tenantId, spaceId,
                                        dependency.getName())).state.equals(BootstrapState.State.OK)) {
                            failed.add(component.getName());
                            log.error(String.format("Component %s's dependency: %s is not met", component.getName(),
                                    dependency.getName()));
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
                            log.info(String.format("Bootstrap status of [%s] is %s, %d out of %d retries remained.",
                                    component.getName(), state.state.toString(), numOfRetries, NUM_RETRIES));
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
