package com.latticeengines.dataplatform.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

public interface JobService {

	List<ApplicationReport> getJobReportsAll();

	ApplicationReport getJobReportById(ApplicationId appId);

	List<ApplicationReport> getJobReportByUser(String user);

	ApplicationId submitJob(String yarnClientName);

	void killJob(ApplicationId appId);

}
