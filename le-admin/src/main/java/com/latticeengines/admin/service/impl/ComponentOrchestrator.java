package com.latticeengines.admin.service.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.common.exposed.graph.traversal.impl.TopologicalTraverse;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

@Component
public class ComponentOrchestrator {

    @Autowired
    List<LatticeComponent> components;

    private static Map<String, LatticeComponent> componentMap;
    private static BatonService batonService = new BatonServiceImpl();

    private static final long TIMEOUT = 180000L;
    private static final long WAIT_INTERVAL = 1000L;
    private static final int NUM_RETRIES = (int) (TIMEOUT/WAIT_INTERVAL);

    private static final Log log = LogFactory.getLog(ComponentOrchestrator.class);

    public ComponentOrchestrator() {}

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

    public List<LatticeComponent> getComponents(){
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

    public Set<String> getServiceNames() { return componentMap.keySet(); }

    public LatticeComponent getComponent(String serviceName) {
        if (componentMap.containsKey(serviceName)) {
            return componentMap.get(serviceName);
        } else {
            return null;
        }
    }

    public void orchestrate(String contractId, String tenantId, String spaceId,
                            Map<String, Map<String, String>> properties) {
        OrchestratorVisitor visitor = new OrchestratorVisitor(contractId, tenantId, spaceId, properties);
        TopologicalTraverse traverser = new TopologicalTraverse();
        traverser.traverse(components, visitor);
    }


    private static class OrchestratorVisitor implements Visitor {
        public Set<String> failed = new HashSet<>();
        public final String contractId;
        public final String tenantId;
        public final String spaceId;
        public final Map<String, Map<String, String>> properties; // serviceName -> bootstrapProerties

        public OrchestratorVisitor(
                String contractId, String tenantId, String spaceId, Map<String, Map<String, String>> properties) {
            this.contractId = contractId;
            this.tenantId = tenantId;
            this.spaceId = spaceId;
            this.properties = properties;
        }

        @Override
        public void visit(Object o, VisitorContext ctx) {
            if (o instanceof LatticeComponent) {
                LatticeComponent component = (LatticeComponent) o;

                log.info("Attempt to install component " + component.getName());

                List<? extends LatticeComponent> dependencies = component.getChildren();
                for(LatticeComponent dependency : dependencies) {
                    if (this.failed.contains(dependency.getName()) ||
                            !(batonService.getTenantServiceBootstrapState(contractId, tenantId, spaceId,
                                    dependency.getName())).state.equals(BootstrapState.State.OK)) {
                        // dependency not satisfied
                        failed.add(component.getName());
                        return;
                    }
                }

                log.info("Dependencies for installing component " + component.getName() + " are all satisfied.");

                if (properties.containsKey(component.getName())) {
                    Map<String, String> bootstrapProperties = properties.get(component.getName());
                    batonService.bootstrap(contractId, tenantId, spaceId, component.getName(), bootstrapProperties);

                    int numOfRetries = NUM_RETRIES;
                    BootstrapState state;
                    do {
                        state = batonService.getTenantServiceBootstrapState(contractId, tenantId, spaceId, component.getName());
                        if (numOfRetries-- % 10 == 0) {
                            log.info(String.format("Bootstrap status of [%s] is %s, %d out of %d retries remained.",
                                    component.getName(), state.state.toString(), numOfRetries, NUM_RETRIES));
                        }
                        try {
                            Thread.sleep(WAIT_INTERVAL);
                        } catch (InterruptedException e) {
                            break;
                        }
                    } while(numOfRetries > 0 && state.state.equals(BootstrapState.State.INITIAL));

                    if (!state.state.equals(BootstrapState.State.OK)) {
                        failed.add(component.getName());
                    }

                }
            }
        }
    }

}
