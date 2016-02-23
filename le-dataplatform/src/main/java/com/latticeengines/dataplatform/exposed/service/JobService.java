package com.latticeengines.dataplatform.exposed.service;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;

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

    void populateJobStatusFromYarnAppReport(JobStatus jobStatus, String applicationId);

    Counters getMRJobCounters(String applicationId);
}
