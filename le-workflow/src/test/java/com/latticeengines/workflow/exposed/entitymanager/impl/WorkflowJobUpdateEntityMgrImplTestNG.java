package com.latticeengines.workflow.exposed.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

public class WorkflowJobUpdateEntityMgrImplTestNG extends WorkflowTestNGBase {
    @Autowired
    private WorkflowJobUpdateEntityMgr workflowJobUpdateEntityMgr;

    private WorkflowJobUpdate jobUpdate1;
    private WorkflowJobUpdate jobUpdate2;
    private Long workflowPid1 = 9901L;
    private Long workflowPid2 = 9902L;
    private Long lastUpdateTime1 = 1516231234L;
    private Long lastUpdateTime2 = 1516235972L;

    @BeforeClass(groups = "functional")
    @Override
    public void setup() {
        jobUpdate1 = new WorkflowJobUpdate();
        jobUpdate1.setWorkflowPid(workflowPid1);
        jobUpdate1.setLastUpdateTime(lastUpdateTime1);
        workflowJobUpdateEntityMgr.create(jobUpdate1);

        jobUpdate2 = new WorkflowJobUpdate();
        jobUpdate2.setWorkflowPid(workflowPid2);
        jobUpdate2.setLastUpdateTime(lastUpdateTime2);
        workflowJobUpdateEntityMgr.create(jobUpdate2);
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        workflowJobUpdateEntityMgr.deleteAll();
    }

    @Test(groups = "functional")
    public void testFindByWorkflowPid() {
        jobUpdate1 = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowPid1);
        Assert.assertEquals(jobUpdate1.getWorkflowPid(), workflowPid1);
        Assert.assertEquals(jobUpdate1.getLastUpdateTime(), lastUpdateTime1);

        jobUpdate2 = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowPid2);
        Assert.assertEquals(jobUpdate2.getWorkflowPid(), workflowPid2);
        Assert.assertEquals(jobUpdate2.getLastUpdateTime(), lastUpdateTime2);
    }

    @Test(groups = "functional", dependsOnMethods = "testFindByWorkflowPid")
    public void testUpdateLastUpdateTime() {
        jobUpdate1.setLastUpdateTime(lastUpdateTime2);
        workflowJobUpdateEntityMgr.updateLastUpdateTime(jobUpdate1);
        WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(jobUpdate1.getWorkflowPid());
        Assert.assertEquals(jobUpdate.getLastUpdateTime(), lastUpdateTime2);
    }

    @Test(groups = "functional", dependsOnMethods = "testUpdateLastUpdateTime")
    public void testFindByLastUpdateTime() {
        List<WorkflowJobUpdate> jobUpdates = workflowJobUpdateEntityMgr.findByLastUpdateTime(lastUpdateTime2);
        Assert.assertEquals(jobUpdates.size(), 2);
        Assert.assertEquals(jobUpdates.get(0).getWorkflowPid(), workflowPid1);
        Assert.assertEquals(jobUpdates.get(0).getLastUpdateTime(), lastUpdateTime2);
        Assert.assertEquals(jobUpdates.get(1).getWorkflowPid(), workflowPid2);
        Assert.assertEquals(jobUpdates.get(1).getLastUpdateTime(), lastUpdateTime2);
    }
}
