package com.latticeengines.admin.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class TenantEntityMgrImplTestNG extends AdminFunctionalTestNGBase {
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    
    @Autowired
    private TestLatticeComponent testLatticeComponent;

    @Test(groups = "functional", timeOut = 5000)
    public void getTenantServiceState() throws Exception {
        CustomerSpaceServiceScope scope = testLatticeComponent.getScope();

        BootstrapState state = null;
        int numTries = 0;
        do {
            state = tenantEntityMgr.getTenantServiceState(scope.getContractId(), scope.getTenantId(), scope.getServiceName());
            Thread.sleep(1000L);
            numTries++;
        } while (state.state != BootstrapState.State.OK && numTries < 5);

        assertEquals(state.installedVersion, 1);
    }

    @Test(groups = "functional", dependsOnMethods = { "getTenantServiceState" })
    public void getTenantServiceConfig() {
        CustomerSpaceServiceScope scope = testLatticeComponent.getScope();
        SerializableDocumentDirectory dir = tenantEntityMgr.getTenantServiceConfig( //
                scope.getContractId(), scope.getTenantId(), scope.getServiceName());
        assertNotNull(dir.getDocumentDirectory());
    }
}
