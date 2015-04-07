package com.latticeengines.admin.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class TenantEntityMgrImplTestNG extends AdminFunctionalTestNGBase {
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    
    @Autowired
    private TestLatticeComponent testLatticeComponent;

    @Test(groups = "functional")
    public void getTenantServiceState() {
        CustomerSpaceServiceScope scope = testLatticeComponent.getScope();
        BootstrapState state = tenantEntityMgr.getTenantServiceState(scope.getContractId(), scope.getTenantId(), scope.getServiceName());
        assertEquals(state.state, BootstrapState.State.OK);
        assertEquals(state.installedVersion, 1);
    }
}
