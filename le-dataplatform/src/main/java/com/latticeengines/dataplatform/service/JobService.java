package com.latticeengines.dataplatform.service;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.latticeengines.domain.exposed.dataplatform.DbCreds;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;

public interface JobService {

    List<ApplicationReport> getJobReportsAll();

    ApplicationReport getJobReportById(ApplicationId appId);

    List<ApplicationReport> getJobReportByUser(String user);

    ApplicationId submitYarnJob(String yarnClientName, Properties appMasterProperties, Properties containerProperties);

    ApplicationId submitMRJob(String mrJobName, Properties properties);

    void killJob(ApplicationId appId);

    ApplicationId submitJob(Job job);

    ApplicationId resubmitPreemptedJob(Job job);

    void createHdfsDirectory(String directory, boolean errorIfExists);

    ApplicationId loadData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols);

    JobStatus getJobStatus(String applicationId);

    ApplicationId loadData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, int numMappers);

}
