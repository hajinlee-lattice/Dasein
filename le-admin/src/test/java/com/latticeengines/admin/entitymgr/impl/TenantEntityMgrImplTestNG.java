package com.latticeengines.admin.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class TenantEntityMgrImplTestNG extends AdminFunctionalTestNGBase {
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TestLatticeComponent testLatticeComponent;

    @Test(groups = "functional", timeOut = 5000)
    public void getTenantServiceState() throws Exception {
        sendOutBootstrapCommand();

        BootstrapState state;
        int numTries = 0;
        do {
            state = tenantEntityMgr.getTenantServiceState(
                    TestContractId, TestTenantId, testLatticeComponent.getName());
            Thread.sleep(1000L);
            numTries++;
        } while (state.state != BootstrapState.State.OK && numTries < 5);

        assertEquals(state.installedVersion, 1);
    }

    @Test(groups = "functional", dependsOnMethods = { "getTenantServiceState" })
    public void getTenantServiceConfig() {
        SerializableDocumentDirectory dir = tenantEntityMgr.getTenantServiceConfig( //
                TestContractId, TestTenantId, testLatticeComponent.getName());
        assertNotNull(dir.getDocumentDirectory());
    }

    private void sendOutBootstrapCommand() {
        String serviceName = testLatticeComponent.getName();
        CustomerSpace space = new CustomerSpace();
        space.setContractId(TestContractId);
        space.setTenantId(TestTenantId);
        space.setSpaceId(CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);

        DocumentDirectory defaultConfig = batonService.getDefaultConfiguration(testLatticeComponent.getName());
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(defaultConfig);
        Map<String, String> bootstrapProperties = sDir.flatten();

        bootstrap(TestContractId, TestTenantId, serviceName, bootstrapProperties);
    }

}
