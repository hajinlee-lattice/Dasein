package com.latticeengines.eai.service.impl.camel;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.route.HdfsToS3RouteConfiguration;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.eai.EaiProxy;

public class HdfsToS3RouteDeploymentTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private HdfsToS3RouteTestNG exportServiceTestNG;

    @Autowired
    private EaiProxy eaiProxy;

    @BeforeClass(groups = "aws")
    public void setup() throws Exception {
        exportServiceTestNG.setup();
    }

    @Test(groups = "aws")
    public void testDownloadSftpByRestCall() throws Exception {
        HdfsToS3RouteConfiguration camelRouteConfiguration =  exportServiceTestNG.getRouteConfiguration();
        ImportConfiguration importConfig =
                ImportConfiguration.createForCamelRouteConfiguration(camelRouteConfiguration);
        AppSubmission submission = eaiProxy.createImportDataJob(importConfig);
        assertNotEquals(submission.getApplicationIds().size(), 0);
        String appId = submission.getApplicationIds().get(0);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, WORKFLOW_WAIT_TIME_IN_MILLIS,
                FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

}
