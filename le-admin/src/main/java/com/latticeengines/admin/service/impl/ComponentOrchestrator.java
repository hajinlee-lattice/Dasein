package com.latticeengines.admin.service.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

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

    private static Set<String> serviceNames;
    private static BatonService batonService = new BatonServiceImpl();

    public ComponentOrchestrator() {}

    public ComponentOrchestrator(List<LatticeComponent> components) {
        this.components = components;
        postConstruct();
    }

    @PostConstruct
    public void postConstruct() {
        Set<String> registered = batonService.getRegisteredServices();
        serviceNames = new HashSet<>();
        for (LatticeComponent component : components) {
            if (!registered.contains(component.getName())) {
                component.register();
            }
            serviceNames.add(component.getName());
        }
    }

    public Set<String> getServiceNames() { return serviceNames; }

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

                if (properties.containsKey(component.getName())) {
                    Map<String, String> bootstrapProperties = properties.get(component.getName());
                    batonService.bootstrap(contractId, tenantId, spaceId, component.getName(), bootstrapProperties);

                    int numOfRetries = 30;
                    BootstrapState state;
                    do {
                        state = batonService.getTenantServiceBootstrapState(contractId, tenantId, spaceId, component.getName());
                        numOfRetries--;
                        try {
                            Thread.sleep(200L);
                        } catch (InterruptedException e) {
                            break;
                        }
                    } while(numOfRetries > 0 && state.state.equals(BootstrapState.State.INITIAL));

                    if (state == null || !state.state.equals(BootstrapState.State.OK)) {
                        failed.add(component.getName());
                    }

                }
            }
        }
    }

}
