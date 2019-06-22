package com.latticeengines.quartz.entitymanager;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.domain.exposed.quartz.JobInfoDetail;

public interface SchedulerEntityMgr {
    Boolean addJob(String tenantId, JobConfig jobConfig);

    Boolean addPredefinedJob(String jobName);

    Boolean deleteJob(String tenantId, String jobName);

    List<JobInfo> listJobs(String tenantId);

    List<JobInfo> listAllJobs();

    JobInfoDetail getJobDetail(String tenantId, String jobName);

    Boolean pauseAllJobs();

    Boolean resumeAllJobs();

    void addPredefinedJob(JobConfig jobConfig);

    Date getNextDateFromCronExpression(String cronExpression);

}
