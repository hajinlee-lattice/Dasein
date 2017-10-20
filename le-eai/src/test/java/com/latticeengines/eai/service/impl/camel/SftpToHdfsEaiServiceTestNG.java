package com.latticeengines.eai.service.impl.camel;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.eai.EaiJob;
import com.latticeengines.domain.exposed.eai.route.SftpToHdfsRouteConfiguration;
import com.latticeengines.eai.functionalframework.EaiMiniClusterFunctionalTestNGBase;
import com.latticeengines.eai.service.EaiYarnService;

@Component("sftpToHdfsEaiServiceTestNG")
public class SftpToHdfsEaiServiceTestNG extends EaiMiniClusterFunctionalTestNGBase {

    @Autowired
    private SftpToHdfsRouteServiceTestNG routeServiceTestNG;

    @Autowired
    private EaiYarnService eaiYarnService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.setup();
        routeServiceTestNG.cleanup();
    }
    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        super.clear();
    }

    @Test(groups = "functional")
    public void testDownloadSftp() throws Exception {
        SftpToHdfsRouteConfiguration camelRouteConfiguration = routeServiceTestNG.getRouteConfiguration();
        EaiJob job = eaiYarnService.createJob(camelRouteConfiguration);
        ApplicationId appId = testYarnJob(job.getClient(), job.getAppMasterPropertiesObject(), job.getContainerPropertiesObject());
        assertNotNull(appId);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        Assert.assertTrue(routeServiceTestNG.waitForFileToBeDownloaded(), "Could not find the file to be downloaded");
    }

}
