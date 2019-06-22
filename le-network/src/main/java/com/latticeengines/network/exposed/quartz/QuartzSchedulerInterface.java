package com.latticeengines.network.exposed.quartz;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.domain.exposed.quartz.JobInfoDetail;

public interface QuartzSchedulerInterface {
    Boolean setSchedulerStatus(String status);

    Boolean addJob(String tenantId, JobConfig jobConfig);

    Boolean deleteJob(String tenantId, String jobName);

    List<JobInfo> listJobs(String tenantId);

    List<JobInfo> listAllJobs();

    JobInfoDetail getJobDetail(String tenantId, String jobName);

    Date getNextDateFromCronExpression(String cronExpression);
}
