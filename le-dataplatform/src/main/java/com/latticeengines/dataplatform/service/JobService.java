package com.latticeengines.dataplatform.service;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.latticeengines.dataplatform.exposed.domain.DbCreds;
import com.latticeengines.dataplatform.exposed.domain.Job;
import com.latticeengines.dataplatform.exposed.domain.JobStatus;

public interface JobService {

    List<ApplicationReport> getJobReportsAll();

    ApplicationReport getJobReportById(ApplicationId appId);

    List<ApplicationReport> getJobReportByUser(String user);

    ApplicationId submitYarnJob(String yarnClientName,
            Properties appMasterProperties, Properties containerProperties);

    ApplicationId submitMRJob(String mrJobName, Properties properties);

    void killJob(ApplicationId appId);
    
    ApplicationId submitJob(Job job);

    ApplicationId resubmitPreemptedJob(Job job);
    
	void createHdfsDirectory(String directory, boolean errorIfExists);

	ApplicationId loadData(String table, String targetDir, DbCreds creds, String queue);

    JobStatus getJobStatus(String applicationId);

}
