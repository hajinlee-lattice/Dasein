package com.latticeengines.eai.service.impl.camel;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.route.SftpToHdfsRouteConfiguration;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.eai.EaiProxy;

@Component("sftpToHdfsEaiServiceDeploymentTestNG")
public class SftpToHdfsEaiServiceDeploymentTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private SftpToHdfsRouteServiceTestNG routeServiceTestNG;

    @Autowired
    private EaiProxy eaiProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        routeServiceTestNG.cleanup();
    }

    @Test(groups = "deployment")
    public void testDownloadSftpByRestCall() throws Exception {
        SftpToHdfsRouteConfiguration camelRouteConfiguration =  routeServiceTestNG.getRouteConfiguration();
        ImportConfiguration importConfig =
                ImportConfiguration.createForCamelRouteConfiguration(camelRouteConfiguration);
        AppSubmission submission = eaiProxy.createImportDataJob(importConfig);
        assertNotEquals(submission.getApplicationIds().size(), 0);
        String appId = submission.getApplicationIds().get(0);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, WORKFLOW_WAIT_TIME_IN_MILLIS,
                FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        Assert.assertTrue(routeServiceTestNG.waitForFileToBeDownloaded(), "Could not find the file to be downloaded");
    }

}
