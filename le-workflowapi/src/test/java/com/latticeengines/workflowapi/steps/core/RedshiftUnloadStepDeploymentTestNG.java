package com.latticeengines.workflowapi.steps.core;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.workflowapi.functionalframework.WorkflowFrameworkDeploymentTestNGBase;

/**
 * admin,pls,lp,cdl,workflowapi,objectapi
 */
public class RedshiftUnloadStepDeploymentTestNG extends WorkflowFrameworkDeploymentTestNGBase {

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Override
    @BeforeClass(groups = "deployment" )
    public void setup() throws Exception {
        setupTestEnvironment(LatticeProduct.CG);
        cdlTestDataService.populateData(mainTestCustomerSpace.getTenantId(), 4);
    }

    @Override
    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test(groups = "deployment")
    public void testWorkflow() throws Exception {
        super.testWorkflow();
        verifyTest();
    }

    @Override
    protected void verifyTest() {
    }

}
