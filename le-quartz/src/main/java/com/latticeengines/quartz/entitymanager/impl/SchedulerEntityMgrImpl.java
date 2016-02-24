package com.latticeengines.quartz.entitymanager.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.validator.routines.UrlValidator;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.domain.exposed.quartz.JobInfoDetail;
import com.latticeengines.quartz.entitymanager.JobHistoryEntityMgr;
import com.latticeengines.quartz.entitymanager.SchedulerEntityMgr;
import com.latticeengines.quartz.service.WorkFlowJob;

@Component("schedulerEntityMgr")
public class SchedulerEntityMgrImpl implements SchedulerEntityMgr {

    private static final Log log = LogFactory.getLog(SchedulerEntityMgrImpl.class);

    @Autowired
    private Scheduler scheduler;

    @Autowired
    private JobHistoryEntityMgr jobHistoryEntityMgr;

    @Override
    public Boolean addJob(String tenantId, JobConfig jobConfig) {
        Boolean added = false;
        JobKey jobKey = new JobKey(jobConfig.getJobName(), tenantId);
        try {
            if (scheduler.checkExists(jobKey)) {
                added = false;
            } else {
                if (!checkUrl(jobConfig.getDestUrl()))
                    throw new LedpException(LedpCode.LEDP_30000,
                            new String[] { jobConfig.getDestUrl() });
                JobDetail jobDetail = JobBuilder
                        .newJob(com.latticeengines.quartz.service.WorkFlowJob.class)
                        .withIdentity(jobKey).build();
                jobDetail.getJobDataMap().put(WorkFlowJob.DESTURL,
                        jobConfig.getDestUrl());
                jobDetail.getJobDataMap().put(WorkFlowJob.JOBARGUMENTS,
                        jobConfig.getJobArguments());
                CronTrigger trigger = TriggerBuilder
                        .newTrigger()
                        .withIdentity(jobConfig.getJobName() + "_trigger",
                                tenantId)
                        .withSchedule(
                                CronScheduleBuilder.cronSchedule(jobConfig
                                        .getCronTrigger())).build();
                scheduler.scheduleJob(jobDetail, trigger);
                added = true;
            }
        } catch (SchedulerException e) {
            added = false;
            log.error(e.getMessage());
        }
        return added;
    }

    @Override
    public Boolean deleteJob(String tenantId, String jobName) {
        Boolean deleted = false;
        try {
            JobKey jobKey = new JobKey(jobName, tenantId);
            if (scheduler.checkExists(jobKey)) {
                scheduler.deleteJob(jobKey);
                deleted = true;
            } else {
                deleted = true;
            }
        } catch (SchedulerException e) {
            deleted = false;
            e.printStackTrace();
        }
        return deleted;
    }

    @Override
    public List<JobInfo> listJobs(String tenantId) {
        List<JobInfo> allJobs = new ArrayList<JobInfo>();
        try {
            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher
                    .jobGroupEquals(tenantId))) {
                String jobName = jobKey.getName();
                JobInfo jobInfo = new JobInfo();
                jobInfo.setJobName(jobName);
                jobInfo.setTenantId(tenantId);
                allJobs.add(jobInfo);
            }
        } catch (SchedulerException e) {
            allJobs.clear();
            log.error(e.getMessage());
        }
        return allJobs;
    }

    @Override
    public List<JobInfo> listAllJobs() {
        List<JobInfo> allJobs = new ArrayList<JobInfo>();
        try {
            for (String groupName : scheduler.getJobGroupNames()) {
                for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher
                        .jobGroupEquals(groupName))) {
                    String jobName = jobKey.getName();
                    JobInfo jobInfo = new JobInfo();
                    jobInfo.setJobName(jobName);
                    jobInfo.setTenantId(groupName);
                    allJobs.add(jobInfo);
                }
            }
        } catch (SchedulerException e) {
            allJobs.clear();
            log.error(e.getMessage());
        }
        return allJobs;
    }

    @SuppressWarnings("unchecked")
    @Override
    public JobInfoDetail getJobDetail(String tenantId, String jobName) {
        JobInfoDetail jobDetail = new JobInfoDetail();
        try {
            JobKey jobKey = new JobKey(jobName, tenantId);
            if (scheduler.checkExists(jobKey)) {
                jobDetail.setJobName(jobKey.getName());
                jobDetail.setTenantId(jobKey.getGroup());
                List<Trigger> triggers = (List<Trigger>) scheduler
                        .getTriggersOfJob(jobKey);
                jobDetail.setNextTriggerTime(triggers.get(0).getNextFireTime());
                List<JobHistory> jobHistories = jobHistoryEntityMgr.getJobHistory(
                        tenantId, jobName);
                jobDetail.setHistoryJobs(jobHistories);
            }
        } catch (SchedulerException e) {
            log.error(e.getMessage());
        }
        return jobDetail;
    }

    @Override
    public Boolean pauseAllJobs() {
        Boolean paused = false;
        try {
            scheduler.pauseAll();
            paused = true;
        } catch (SchedulerException e) {
            paused = false;
            log.error(e.getMessage());
        }
        return paused;
    }

    @Override
    public Boolean resumeAllJobs() {
        Boolean resumed = false;
        try {
            scheduler.resumeAll();
            resumed = true;
        } catch (SchedulerException e) {
            resumed = false;
            log.error(e.getMessage());
        }
        return resumed;
    }

    private Boolean checkUrl(String url) {
        String[] schemes = { "http", "https" };
        UrlValidator urlValidator = new UrlValidator(schemes);
        return urlValidator.isValid(url);
    }
}
