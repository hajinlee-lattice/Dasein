package com.latticeengines.dataplatform.service;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;

public interface JobService {

    List<ApplicationReport> getJobReportsAll();

    ApplicationReport getJobReportById(ApplicationId appId);

    List<ApplicationReport> getJobReportByUser(String user);

    ApplicationId submitYarnJob(String yarnClientName, Properties appMasterProperties, Properties containerProperties);

    ApplicationId submitMRJob(String mrJobName, Properties properties);

    void killJob(ApplicationId appId);

    void createHdfsDirectory(String directory, boolean errorIfExists);

    ApplicationId submitJob(Job job);

    JobStatus getJobStatus(String applicationId);
}
