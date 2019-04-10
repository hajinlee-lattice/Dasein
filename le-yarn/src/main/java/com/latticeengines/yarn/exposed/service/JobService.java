package com.latticeengines.yarn.exposed.service;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.yarn.client.CommandYarnClient;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;

public interface JobService extends AwsBatchJobService {

    List<ApplicationReport> getJobReportsAll();

    ApplicationReport getJobReportById(ApplicationId appId);

    List<ApplicationReport> getJobReportByUser(String user);

    ApplicationId submitYarnJob(String yarnClientName, Properties appMasterProperties, Properties containerProperties);

    ApplicationId submitMRJob(String mrJobName, Properties properties);

    void killJob(ApplicationId appId);

    void createHdfsDirectory(String directory, boolean errorIfExists);

    ApplicationId submitJob(Job job);

    JobStatus getJobStatus(String applicationId);

    JobStatus getJobStatusByCluster(String applicationId, String clusterId);

    JobStatus waitFinalJobStatus(String applicationId, Integer timeoutInSec);

    void populateJobStatusFromYarnAppReport(JobStatus jobStatus, String applicationId);

    Counters getMRJobCounters(String applicationId);

    static JobID runMRJob(org.apache.hadoop.mapreduce.Job job, String mrJobName, boolean waitForCompletion)
            throws Exception {
        return runMRJob(job, mrJobName, waitForCompletion, null, null);
    }

    static JobID runMRJob(org.apache.hadoop.mapreduce.Job job, String mrJobName, boolean waitForCompletion,
            String counterGroupName, Map<String, Long> counterGroupResultMap) throws Exception {
        JobRunner runner = new JobRunner();
        runner.setJob(job);
        runner.setWaitForCompletion(waitForCompletion);
        runner.call();
        if (waitForCompletion //
                && StringUtils.isNotBlank(counterGroupName) //
                && counterGroupResultMap != null) {
            Iterator<Counter> itr = job.getCounters().getGroup(counterGroupName).iterator();
            while (itr.hasNext()) {
                Counter c = itr.next();
                counterGroupResultMap.put(c.getName(), c.getValue());
            }
        }
        return job.getJobID();
    }

    YarnClient getYarnClient(String yarnClientName);

    ApplicationId submitYarnJob(CommandYarnClient yarnClient, String yarnClientName, Properties appMasterProperties,
            Properties containerProperties);

    String getEmrClusterId();

}
