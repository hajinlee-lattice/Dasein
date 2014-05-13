package com.latticeengines.dataplatform.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
import com.latticeengines.dataplatform.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.yarn.client.ContainerProperty;

public class YarnServiceImplTestNG extends DataPlatformFunctionalTestNGBase {
    
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
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), "Priority0.0");
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), "Dell");
        Properties containerProperties = new Properties();
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "64");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");
        ApplicationId applicationId = jobService.submitYarnJob("defaultYarnClient", appMasterProperties,
                containerProperties);
        AppInfo appInfo = yarnService.getApplication(applicationId.toString());
        assertNotNull(appInfo);
        
        YarnApplicationState state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
        assertEquals(YarnApplicationState.FINISHED, state);
    }
    
}
