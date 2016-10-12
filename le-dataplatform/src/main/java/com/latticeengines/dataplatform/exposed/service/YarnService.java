package com.latticeengines.dataplatform.exposed.service;

import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;

public interface YarnService {

    SchedulerTypeInfo getSchedulerInfo();

    CapacitySchedulerInfo getCapacitySchedulerInfo();

    AppsInfo getApplications(String queryString);

    AppInfo getApplication(String appId);

    List<AppInfo> getPreemptedApps();

}
