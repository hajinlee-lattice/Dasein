package com.latticeengines.apps.cdl.testframework;

import java.util.Map;

import javax.annotation.Resource;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

public abstract class CDLWorkflowFrameworkDeploymentTestNGBase extends CDLWorkflowFrameworkTestNGBase {

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed testBed;

    protected void setupTestEnvironment() {
        try {
            setupEnd2EndTestEnvironment(null);
            mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
            setupYarnPlatform();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void setupYarnPlatform() {
        platformTestBase = new YarnFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
    }

    protected void setupEnd2EndTestEnvironment(Map<String, Boolean> featureFlagMap) throws Exception {
        setupTestEnvironmentWithFeatureFlags(featureFlagMap);
        mainTestTenant = testBed.getMainTestTenant();
        mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
    }

}
