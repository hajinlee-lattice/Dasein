package com.latticeengines.admin.tenant.batonadapter.testcomponent;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

import junit.framework.Assert;

public class TestComponentFunctionalTestNG extends AdminFunctionalTestNGBase {

    @Autowired
    private TestLatticeComponent component;

    @Autowired
    private TenantService tenantService;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();

        try {
            batonService.discardService(component.getName());
        } catch (Exception e) {
            // ignore
        }
        component.register();
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        batonService.discardService(component.getName());
    }

    @Test(groups = "functional")
    public void bootstrap() {
        sendOutBootstrapCommand();

        int numOfRetries = 5;
        boolean stateIsOK;
        do {
            BootstrapState.State state =
                    batonService.getTenantServiceBootstrapState(
                            TestContractId, TestTenantId, component.getName()).state;
            stateIsOK = state.equals(BootstrapState.State.OK);
            numOfRetries--;
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new AssertionError("Checking bootstrap state interrupted", e);
            }
        } while (numOfRetries > 0 && !stateIsOK);

        Assert.assertTrue(stateIsOK);

        Assert.assertNotNull(tenantService.getTenantServiceConfig(TestContractId, TestTenantId, component.getName()));
    }

    private void sendOutBootstrapCommand() {
        String serviceName = component.getName();

        DocumentDirectory defaultConfig = batonService.getDefaultConfiguration(component.getName());
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(defaultConfig);
        Map<String, String> bootstrapProperties = sDir.flatten();

        bootstrap(TestContractId, TestTenantId, serviceName, bootstrapProperties);
    }

}
