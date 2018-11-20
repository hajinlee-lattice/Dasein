package com.latticeengines.workflowapi.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSBatchConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflowapi.WorkflowLogLinks;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.service.WorkflowContainerService;

public class WorkflowContainerServiceImplTestNG extends WorkflowApiFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(WorkflowContainerServiceImplTestNG.class);

    @Inject
    private WorkflowContainerService workflowContainerService;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    private WorkflowJob workflowJob;

    @AfterMethod(groups = "functional")
    public void clearup() {
        workflowJobEntityMgr.delete(workflowJob);
    }

    @Test(groups = "functional", enabled = true)
    public void testSubmitAwsWorkflow() {
        WorkflowConfiguration workflowConfig = new WorkflowConfiguration();
        workflowConfig.setWorkflowName("dummyWorkflow");
        workflowConfig.setCustomerSpace(WFAPITEST_CUSTOMERSPACE);

        AWSBatchConfiguration awsConfig = new AWSBatchConfiguration();
        awsConfig.setCustomerSpace(WFAPITEST_CUSTOMERSPACE);
        awsConfig.setMicroServiceHostPort("https://localhost");
        awsConfig.setRunInAws(true);
        workflowConfig.add(awsConfig);
        BaseStepConfiguration baseConfig = new BaseStepConfiguration();
        workflowConfig.add(baseConfig);

        String applicationId = workflowContainerService.submitAwsWorkflow(workflowConfig, null);
        Assert.assertNotNull(applicationId);
        workflowJob = workflowJobEntityMgr.findByApplicationId(applicationId);
        Assert.assertNotNull(workflowJob);

        Assert.assertNull(workflowContainerService.submitAwsWorkflow(workflowConfig, 9999L));
    }

    @Test(groups = "mannual")
    public void testGetWorkflowLogLink() {
        long workflowPid = 2L;
        WorkflowLogLinks logLinks = workflowContainerService.getLogUrlByWorkflowPid(workflowPid);
        System.out.println(logLinks);
    }

}
