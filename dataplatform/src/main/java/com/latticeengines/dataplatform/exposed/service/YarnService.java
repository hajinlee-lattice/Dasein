package com.latticeengines.dataplatform.exposed.service;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;

public interface YarnService {

	SchedulerTypeInfo getSchedulerInfo();

	AppsInfo getApplications();

}
