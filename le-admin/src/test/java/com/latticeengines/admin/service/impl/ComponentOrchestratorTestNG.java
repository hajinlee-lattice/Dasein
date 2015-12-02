package com.latticeengines.admin.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.service.impl.TenantServiceImpl.ProductAndExternalAdminInfo;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class ComponentOrchestratorTestNG extends AdminFunctionalTestNGBase {

    @Autowired
    private ComponentOrchestrator orchestrator;

    @Autowired
    private TenantService tenantService;

    List<LatticeComponent> originalComponents;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        originalComponents = orchestrator.components;

        TestLatticeComponent component1 = new TestLatticeComponent();
        component1.componentName = "Component1";

        TestLatticeComponent component2 = new TestLatticeComponent();
        component2.componentName = "Component2";

        TestLatticeComponent component3 = new TestLatticeComponent();
        component3.componentName = "Component3";

        TestLatticeComponent component4 = new TestLatticeComponent(false);
        component4.componentName = "Component4";

        TestLatticeComponent component5 = new TestLatticeComponent();
        component5.componentName = "Component5";

        TestLatticeComponent component6 = new TestLatticeComponent();
        component6.componentName = "Component6";

        component1.setDependencies(Arrays.asList(component2, component3, component4));
        component3.setDependencies(Collections.singleton(component5));
        component4.setDependencies(Collections.singleton(component5));

        orchestrator = new ComponentOrchestrator(Arrays.asList((LatticeComponent) component1, component2, component3,
                component4, component5, component6));
    }

    @AfterMethod(groups = "functional")
    public void afterMethod() {
        orchestrator = new ComponentOrchestrator(originalComponents);
    }

    @Test(groups = "functional")
    public void getServiceNames() throws Exception {
        orchestrator = new ComponentOrchestrator(originalComponents);
        for (String name : Arrays.asList("BardJams", "PLS", "DLTemplate", "VisiDBDL", "Dante", "VisiDBTemplate")) {
            Assert.assertTrue(orchestrator.getServiceNames().contains(name));
        }
    }

    @Test(groups = "functional")
    public void testGetServiceNamesWithProducts() {
        orchestrator = new ComponentOrchestrator(originalComponents);
        Map<String, Set<LatticeProduct>> serviceProductsMap = orchestrator.getServiceNamesWithProducts();
        Set<String> serviceNameSet = serviceProductsMap.keySet();
        Assert.assertTrue(serviceNameSet.containsAll(orchestrator.getServiceNames()));
        for (String serviceName : serviceNameSet) {
            Set<LatticeProduct> products = serviceProductsMap.get(serviceName);
            LatticeComponent component = orchestrator.getComponent(serviceName);
            Assert.assertNotNull(products);
            Assert.assertEquals(products, component.getAssociatedProducts());
        }
    }

    @Test(groups = "functional")
    public void constructByComponents() throws Exception {
        for (String name : Arrays.asList("Component1", "Component2", "Component3", "Component4", "Component5",
                "Component6")) {
            Assert.assertTrue(orchestrator.getServiceNames().contains(name));
        }
    }

    @Test(groups = "functional")
    public void component1ShouldSuccess() throws Exception {
        DocumentDirectory dir = batonService.getDefaultConfiguration("Component1");
        tenantService.bootstrap(TestContractId, TestTenantId, "Component1",
                new SerializableDocumentDirectory(dir).flatten());

        int numOfRetries = 40;
        BootstrapState state;
        do {
            state = tenantService.getTenantServiceState(TestContractId, TestTenantId, "Component1");
            numOfRetries--;
            Thread.sleep(500L);
        } while (numOfRetries > 0 && !state.state.equals(BootstrapState.State.OK));
        Assert.assertNotNull(state);
        Assert.assertEquals(state.state, BootstrapState.State.OK);
    }

    @Test(groups = "functional")
    public void component4ShouldFail() throws Exception {
        DocumentDirectory dir = batonService.getDefaultConfiguration("Component4");
        tenantService.bootstrap(TestContractId, TestTenantId, "Component4",
                new SerializableDocumentDirectory(dir).flatten());

        int numOfRetries = 40;
        BootstrapState state;
        do {
            state = tenantService.getTenantServiceState(TestContractId, TestTenantId, "Component4");
            numOfRetries--;
            Thread.sleep(500L);
        } while (numOfRetries > 0 && !state.state.equals(BootstrapState.State.ERROR));
        Assert.assertNotNull(state);
        Assert.assertEquals(state.state, BootstrapState.State.ERROR);
    }

    @Test(groups = "functional")
    public void emptyNodesShouldSuccess() throws Exception {
        tenantService.bootstrap(TestContractId, TestTenantId, "Component1", new HashMap<String, String>());
        int numOfRetries = 40;
        BootstrapState state;
        do {
            state = tenantService.getTenantServiceState(TestContractId, TestTenantId, "Component1");
            numOfRetries--;
            Thread.sleep(500L);
        } while (numOfRetries > 0 && !state.state.equals(BootstrapState.State.OK));
        Assert.assertNotNull(state);
        Assert.assertEquals(state.state, BootstrapState.State.OK);
    }

    @Test(groups = "functional")
    public void orchestrationWithDependencies() throws Exception {
        try {
            deleteTenant(TestContractId, TestTenantId);
        } catch (Exception e) {
            // ignore
        }
        createTenant(TestContractId, TestTenantId);

        Map<String, Map<String, String>> properties = new HashMap<>();
        for (LatticeComponent component : orchestrator.components) {
            SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(
                    batonService.getDefaultConfiguration(component.getName()));
            properties.put(component.getName(), sDir.flatten());
        }

        ProductAndExternalAdminInfo prodAndExternalAminInfo = super.generateProductAndExternalAdminInfo();
        orchestrator.orchestrate(TestContractId, TestTenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, properties,
                prodAndExternalAminInfo);

        ExecutorService executorService = Executors.newFixedThreadPool(orchestrator.components.size());
        Map<String, Future<BootstrapState>> states = new HashMap<>();
        for (final LatticeComponent component : orchestrator.components) {
            Future<BootstrapState> future = executorService.submit(new Callable<BootstrapState>() {
                private final String componentName = component.getName();

                @Override
                public BootstrapState call() throws Exception {
                    int numOfRetries = 30;
                    BootstrapState state;
                    do {
                        state = batonService
                                .getTenantServiceBootstrapState(TestContractId, TestTenantId, componentName);
                        numOfRetries--;
                        try {
                            Thread.sleep(200L);
                        } catch (InterruptedException e) {
                            break;
                        }
                    } while (numOfRetries > 0 && state.state.equals(BootstrapState.State.INITIAL));
                    return state;
                }
            });

            states.put(component.getName(), future);
        }

        for (Map.Entry<String, Future<BootstrapState>> entry : states.entrySet()) {
            switch (entry.getKey()) {
            case "Component1":
                Assert.assertEquals(entry.getValue().get().state, BootstrapState.State.INITIAL);
                break;
            case "Component4":
                Assert.assertEquals(entry.getValue().get().state, BootstrapState.State.ERROR);
                break;
            default:
                Assert.assertEquals(entry.getValue().get().state, BootstrapState.State.OK);
            }
        }
    }
}
