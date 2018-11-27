package com.latticeengines.admin.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.entitymgr.ServiceConfigEntityMgr;
import com.latticeengines.admin.entitymgr.TenantConfigEntityMgr;
import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.documentdb.entity.ServiceConfigEntity;
import com.latticeengines.documentdb.entity.TenantConfigEntity;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.security.TenantStatus;

public class TenantEntityMgrImplTestNG extends AdminFunctionalTestNGBase {

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TestLatticeComponent testLatticeComponent;

    @Autowired
    private TenantConfigEntityMgr tenantConfigEntityMgr;

    @Autowired
    private ServiceConfigEntityMgr serviceConfigEntityMgr;

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

    @Test(groups = "functional")
    public void testTenantConfigCRUD() throws Exception {
        ContractProperties contractProperties = new ContractProperties();
        TenantProperties tenantProperties = new TenantProperties();
        CustomerSpaceProperties customerSpaceProperties = new CustomerSpaceProperties();
        FeatureFlagValueMap featureFlags = new FeatureFlagValueMap();
        SpaceConfiguration spaceConfiguration = new SpaceConfiguration();
        tenantConfigEntityMgr.createTenant(TestContractId, TestTenantId, contractProperties, tenantProperties,
                customerSpaceProperties,
                featureFlags,
                spaceConfiguration);
        // wait 500ms replication lag
        Thread.sleep(500);

        TenantConfigEntity tenantEntity = tenantConfigEntityMgr.getTenant(TestContractId, TestContractId);
        Assert.assertEquals(tenantEntity.getStatus(), TenantStatus.INITING);
        Assert.assertNotNull(tenantEntity);
        tenantConfigEntityMgr.updateTenantStatus(tenantEntity.getPid(), TenantStatus.ACTIVE);
        // wait 500ms replication lag
        Thread.sleep(500);

        tenantEntity = tenantConfigEntityMgr.getTenant(TestContractId, TestContractId);
        Assert.assertEquals(tenantEntity.getStatus(), TenantStatus.ACTIVE);

        // verify service config
        serviceConfigEntityMgr.createServiceForTenant(tenantEntity, testLatticeComponent.getName(),
                new SerializableDocumentDirectory(), BootstrapState.createInitialState());
        // wait 500ms replication lag
        Thread.sleep(500);
        ServiceConfigEntity serviceConfigEntity = serviceConfigEntityMgr.getTenantService(tenantEntity,
                testLatticeComponent.getName());
        Assert.assertNotNull(serviceConfigEntity);
        Assert.assertEquals(serviceConfigEntity.getServiceName(), testLatticeComponent.getName());

        tenantConfigEntityMgr.deleteTenant(TestContractId, TestTenantId);

        // wait 500ms replication lag
        Thread.sleep(500);
        tenantEntity = tenantConfigEntityMgr.getTenant(TestContractId, TestContractId);
        Assert.assertNull(tenantEntity);

        serviceConfigEntity = serviceConfigEntityMgr.getTenantService(tenantEntity, testLatticeComponent.getName());
        Assert.assertNull(serviceConfigEntity);

    }
}
