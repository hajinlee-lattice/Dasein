package com.latticeengines.yarn.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.service.JobService;
import com.latticeengines.yarn.exposed.service.YarnService;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

public class YarnServiceImplTestNG extends YarnFunctionalTestNGBase {

    @Autowired
    private JobService jobService;

    @Autowired
    private YarnService yarnService;

    @Test(groups = { "functionalm" })
    public void getSchedulerInfo() {
        SchedulerTypeInfo schedulerInfo = yarnService.getSchedulerInfo();
        assertNotNull(schedulerInfo);
    }

    @Test(groups = { "functional" })
    public void getCapacitySchedulerInfo() {
        CapacitySchedulerInfo schedulerInfo = yarnService.getCapacitySchedulerInfo();
        assertNotNull(schedulerInfo);
    }

    @Test(groups = { "functional" })
    public void getApps() {
        List<ApplicationReport> appReports = yarnService.getApplications(GetApplicationsRequest.newInstance());
        assertNotNull(appReports);
    }

    @Test(groups = { "functional" })
    public void getApp() throws Exception {
        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getModelingQueueNameForSubmission());
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), "Dell-yarn");
        Properties containerProperties = new Properties();
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "64");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");
        ApplicationId applicationId = jobService.submitYarnJob("defaultYarnClient", appMasterProperties,
                containerProperties);
        ApplicationReport appReport = yarnService.getApplication(applicationId.toString());
        assertNotNull(appReport);

        FinalApplicationStatus status = waitForStatus(applicationId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = { "functional" })
    public void getApplicationReportAndKillJob() throws Exception {
        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getModelingQueueNameForSubmission());
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), "Dell-yarn");
        Properties containerProperties = new Properties();
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "64");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");
        ApplicationId applicationId = jobService.submitYarnJob("defaultYarnClient", appMasterProperties,
                containerProperties);
        ApplicationReport applicationReport = jobService.getJobReportById(applicationId);
        ApplicationReport appReport = yarnService.getApplication(applicationId.toString());
        assertNotNull(appReport);
        assertNotNull(applicationReport);

        jobService.killJob(applicationId);

        FinalApplicationStatus status = waitForStatus(applicationId, FinalApplicationStatus.KILLED);
        assertEquals(status, FinalApplicationStatus.KILLED);
    }

}
