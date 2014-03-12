package com.latticeengines.dataplatform.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;

public class YarnServiceImplTestNG extends DataPlatformFunctionalTestNGBase {
    
    private static final Log log = LogFactory.getLog(YarnServiceImplTestNG.class);

    @Autowired
    private JobService jobService;
    
    @Autowired
    private YarnService yarnService;

    @Override
    protected boolean doYarnClusterSetup() {
        return false;
    }

    @Test(groups = "functional")
    public void getSchedulerInfo() {
        SchedulerTypeInfo schedulerInfo = yarnService.getSchedulerInfo();
        assertNotNull(schedulerInfo);
    }

    @Test(groups = "functional")
    public void getApps() {
        AppsInfo appsInfo = yarnService.getApplications(null);
        assertNotNull(appsInfo);
    }
    
    @Test(groups = "functional")
    public void getApp() throws Exception {
        Properties appMasterProperties = new Properties();
        appMasterProperties.put("QUEUE", "Priority0.A");
        Properties containerProperties = new Properties();
        containerProperties.put("VIRTUALCORES", "1");
        containerProperties.put("MEMORY", "64");
        containerProperties.put("PRIORITY", "0");
        ApplicationId applicationId = jobService.submitYarnJob("defaultYarnClient", appMasterProperties,
                containerProperties);
        AppInfo appInfo = yarnService.getApplication(applicationId.toString());
        assertNotNull(appInfo);
        
        YarnApplicationState state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
        assertEquals(YarnApplicationState.FINISHED, state);
    }
    
}
