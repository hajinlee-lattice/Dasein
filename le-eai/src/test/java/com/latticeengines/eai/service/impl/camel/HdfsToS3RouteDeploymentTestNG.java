package com.latticeengines.eai.service.impl.camel;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.route.HdfsToS3Configuration;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.eai.EaiProxy;

public class HdfsToS3RouteDeploymentTestNG extends EaiFunctionalTestNGBase {

    public static final String TEST_CUSTOMER = "EaiTester";

    @Autowired
    private HdfsToS3RouteTestNG hdfsToS3RouteTestNG;

    @Autowired
    private EaiProxy eaiProxy;

    @Value("${common.le.stack}")
    private String leStack;

    private CustomerSpace customerSpace;

    @BeforeClass(groups = "aws")
    public void setup() throws Exception {
        hdfsToS3RouteTestNG.setup();
        customerSpace = CustomerSpace.parse(leStack + "_" + TEST_CUSTOMER);
    }

    @Test(groups = "aws", enabled = false)
    public void testDownloadSftpByRestCall() throws Exception {
        HdfsToS3Configuration camelRouteConfiguration = hdfsToS3RouteTestNG.getRouteConfiguration();
        camelRouteConfiguration.setCustomerSpace(customerSpace);
        AppSubmission submission = eaiProxy.submitEaiJob(camelRouteConfiguration);
        assertNotEquals(submission.getApplicationIds().size(), 0);
        String appId = submission.getApplicationIds().get(0);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, WORKFLOW_WAIT_TIME_IN_MILLIS,
                FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

}
