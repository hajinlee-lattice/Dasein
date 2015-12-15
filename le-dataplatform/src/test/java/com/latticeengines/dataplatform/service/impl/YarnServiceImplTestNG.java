package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class YarnServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private ModelingJobService modelingJobService;

    @Autowired
    private YarnService yarnService;

    @Test(groups = {"functional.platform", "functional.production"})
    public void getSchedulerInfo() {
        SchedulerTypeInfo schedulerInfo = yarnService.getSchedulerInfo();
        assertNotNull(schedulerInfo);

    }

    @Test(groups = {"functional.platform", "functional.production"})
    public void getApps() {
        AppsInfo appsInfo = yarnService.getApplications(null);
        assertNotNull(appsInfo);
    }

    @Test(groups = {"functional.platform", "functional.production"})
    public void getApp() throws Exception {
        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getModelingQueueNameForSubmission());
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), "Dell-" + suffix);
        Properties containerProperties = new Properties();
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "64");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");
        ApplicationId applicationId = modelingJobService.submitYarnJob("defaultYarnClient", appMasterProperties,
                containerProperties);
        AppInfo appInfo = yarnService.getApplication(applicationId.toString());
        assertNotNull(appInfo);

        FinalApplicationStatus status = waitForStatus(applicationId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

}
