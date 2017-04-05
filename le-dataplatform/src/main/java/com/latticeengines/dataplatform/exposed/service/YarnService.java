package com.latticeengines.dataplatform.exposed.service;

import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;

public interface YarnService {

    List<ApplicationReport> getApplications(GetApplicationsRequest request);

    ApplicationReport getApplication(String appId);

    List<ApplicationReport> getPreemptedApps();

    List<ApplicationReport> getRunningApplications(GetApplicationsRequest request);

    CapacitySchedulerInfo getCapacitySchedulerInfo();

    SchedulerTypeInfo getSchedulerInfo();

}
