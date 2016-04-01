package com.latticeengines.eai.service.impl.camel;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.route.SftpToHdfsRouteConfiguration;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;

@Component("sftpToHdfsEaiServiceTestNG")
public class SftpToHdfsEaiServiceTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private SftpToHdfsRouteServiceTestNG routeServiceTestNG;

    @Autowired
    private EaiService eaiService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        routeServiceTestNG.cleanup();
    }

    @Test(groups = "functional")
    public void testDownloadSftp() throws Exception {
        SftpToHdfsRouteConfiguration camelRouteConfiguration =  routeServiceTestNG.getRouteConfiguration();
        ImportConfiguration importConfig =
                ImportConfiguration.createForCamelRouteConfiguration(camelRouteConfiguration);
        ApplicationId appId = eaiService.extractAndImport(importConfig);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        Assert.assertTrue(routeServiceTestNG.waitForFileToBeDownloaded(), "Could not find the file to be downloaded");
    }

}
