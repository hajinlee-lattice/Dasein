package com.latticeengines.apps.cdl.controller;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;

public class CDLDataCleanupResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Autowired
    private CDLProxy cdlProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @BeforeClass(groups = { "deployment" }, enabled = false)
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = { "deployment" }, enabled = false)
    public void testCleanup() {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
        dataFeedProxy.updateDataFeedStatus(customerSpace.toString(), DataFeed.Status.Active.getName());

        ApplicationId appId = cdlProxy.cleanupAll(customerSpace.toString(), null, MultiTenantContext.getEmailAddress());

        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
    }
}
