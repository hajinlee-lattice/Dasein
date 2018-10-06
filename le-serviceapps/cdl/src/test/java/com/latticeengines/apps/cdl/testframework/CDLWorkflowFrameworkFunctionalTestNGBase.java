package com.latticeengines.apps.cdl.testframework;

import static org.testng.Assert.assertNotNull;

import javax.annotation.Resource;
import javax.inject.Inject;


import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataCollection;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

public abstract class CDLWorkflowFrameworkFunctionalTestNGBase extends CDLWorkflowFrameworkTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLWorkflowFrameworkFunctionalTestNGBase.class);

    @Resource(name = "globalAuthFunctionalTestBed")
    protected GlobalAuthFunctionalTestBed testBed;

    @Inject
    protected DataCollectionEntityMgr dataCollectionEntityMgr;

    protected DataCollection dataCollection;
    protected String collectionName;

    public void setup() throws Exception {
        setupTestEnvironmentWithDataCollection();
    }

    protected void setupTestEnvironmentWithDataCollection() throws Exception {
        testBed.bootstrap(1);
        mainTestTenant = testBed.getMainTestTenant();
        mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
        MultiTenantContext.setTenant(mainTestTenant);
        assertNotNull(MultiTenantContext.getTenant());
        testBed.switchToSuperAdmin();
        setupYarnPlatform();

        dataCollection = dataCollectionEntityMgr.createDefaultCollection();
        collectionName = dataCollection.getName();
    }

    protected void setupYarnPlatform() {
        platformTestBase = new YarnFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
    }

}
