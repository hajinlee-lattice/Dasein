package com.latticeengines.admin.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class TenantEntityMgrImplTestNG extends AdminFunctionalTestNGBase {

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TestLatticeComponent testLatticeComponent;

    @Test(groups = "functional", timeOut = 10000)
    public void getTenantServiceState() throws Exception {
        bootstrap(TestContractId, TestTenantId, testLatticeComponent.getName());

        BootstrapState state;
        int numTries = 0;
        do {
            state = tenantEntityMgr.getTenantServiceState(TestContractId, TestTenantId, testLatticeComponent.getName());
            Thread.sleep(1000L);
            numTries++;
        } while (state.state != BootstrapState.State.OK && numTries < 10);

        assertEquals(state.installedVersion, 1);
    }

    @Test(groups = "functional", dependsOnMethods = { "getTenantServiceState" })
    public void getTenantServiceConfig() {
        SerializableDocumentDirectory dir = tenantEntityMgr.getTenantServiceConfig( //
                TestContractId, TestTenantId, testLatticeComponent.getName());
        assertNotNull(dir.getDocumentDirectory());
    }

    @Test(groups = "functional")
    public void getDefaultSpaceConfig() {
        SpaceConfiguration dir = tenantEntityMgr.getDefaultSpaceConfig();
        assertNotNull(dir);
        assertNotNull(dir.toDocumentDirectory());
        assertNotNull(dir.toSerializableDocumentDirectory());
    }
}
