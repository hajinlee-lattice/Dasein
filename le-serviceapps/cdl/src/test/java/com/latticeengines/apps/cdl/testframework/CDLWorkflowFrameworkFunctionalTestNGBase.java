package com.latticeengines.apps.cdl.testframework;

import static org.testng.Assert.assertNotNull;

import javax.annotation.Resource;
import javax.inject.Inject;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

public abstract class CDLWorkflowFrameworkFunctionalTestNGBase extends CDLWorkflowFrameworkTestNGBase {

    @Resource(name = "globalAuthFunctionalTestBed")
    protected GlobalAuthFunctionalTestBed testBed;

    @Inject
    protected DataCollectionEntityMgr dataCollectionEntityMgr;

    protected DataCollection dataCollection;

    protected void setupTestEnvironment() {
        setupTestEnvironmentWithDataCollection();
    }

    protected void setupTestEnvironmentWithDataCollection() {
        testBed.bootstrap(1);
        mainTestTenant = testBed.getMainTestTenant();
        mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
        MultiTenantContext.setTenant(mainTestTenant);
        assertNotNull(MultiTenantContext.getTenant());
        testBed.switchToSuperAdmin();
        setupYarnPlatform();

        dataCollection = dataCollectionEntityMgr.createDefaultCollection();
    }

    private void setupYarnPlatform() {
        platformTestBase = new YarnFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
    }

}
