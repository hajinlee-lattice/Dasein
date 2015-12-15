package com.latticeengines.workflowapi.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.TestApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.YarnAppWorkflowId;
import com.latticeengines.workflowapi.entitymgr.YarnAppWorkflowIdEntityMgr;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class YarnAppWorkflowIdEntityMgrFunctionalTestNG extends WorkflowApiFunctionalTestNGBase {

    @Autowired
    private YarnAppWorkflowIdEntityMgr yarnAppWorkflowIdEntityMgr;

    @Test(groups = "functional")
    public void testFindWorkflowIdByApplicationId() throws Exception {
        ApplicationId appId = platformTestBase.getApplicationId("application_1444871575629_24513");
        WorkflowExecutionId workflowId = new WorkflowExecutionId(549L);

        YarnAppWorkflowId yarnAppWorkflowId = new YarnAppWorkflowId(appId, workflowId);

        try {
            yarnAppWorkflowIdEntityMgr.create(yarnAppWorkflowId);
            WorkflowExecutionId retrievedWorkflowId = yarnAppWorkflowIdEntityMgr.findWorkflowIdByApplicationId(appId);
            assertEquals(retrievedWorkflowId, workflowId);
        } finally {
            yarnAppWorkflowIdEntityMgr.delete(yarnAppWorkflowId);
        }
    }

    public void testNullCase() {
        TestApplicationId appId = new TestApplicationId();
        appId.setId(5);
        appId.setClusterTimestamp(123456L);
        appId.build();

        WorkflowExecutionId retrievedWorkflowId = yarnAppWorkflowIdEntityMgr.findWorkflowIdByApplicationId(appId);
        assertNull(retrievedWorkflowId);
    }

}
