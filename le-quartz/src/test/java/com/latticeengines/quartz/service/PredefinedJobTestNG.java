package com.latticeengines.quartz.service;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.domain.exposed.quartz.JobInfoDetail;
import com.latticeengines.quartz.entitymanager.SchedulerEntityMgr;

@ContextConfiguration(locations = { "classpath:test-quartz-context.xml" })
public class PredefinedJobTestNG extends AbstractTestNGSpringContextTests {

    private static final String JOB_NAME = "testPredefinedJob";
    private static final String JOB_GROUP = "PredefinedJobs";

    @Inject
    private Scheduler scheduler;

    @Inject
    private SchedulerEntityMgr schedulerEntityMgr;

    @Inject
    private ApplicationContext appContext;

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void addJob() {
        List<JobConfig> jobConfigs = (List<JobConfig>) appContext.getBean("testPredefinedJobs");
        for (JobConfig jobConfig : jobConfigs) {
            schedulerEntityMgr.addPredefinedJob(jobConfig);
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "addJob" })
    public void getJobList() {
        List<JobInfo> jobInfos = schedulerEntityMgr.listJobs(JOB_GROUP);
        assertTrue(jobInfos.size() >= 1);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "addJob" })
    public void triggerJob() {
        JobKey jobKey = new JobKey(JOB_NAME, JOB_GROUP);
        try {
            scheduler.triggerJob(jobKey);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
        List<TriggerJobThread> jobList = new ArrayList<>();
        for (int i = 0; i < 5; i ++) {
            jobList.add(new TriggerJobThread(jobKey));
        }
        JobInfoDetail jobDetail = schedulerEntityMgr.getJobDetail(JOB_GROUP, JOB_NAME);
        List<JobHistory> jobHistories = jobDetail.getHistoryJobs();
        if (jobHistories != null) {
            for (JobHistory jobHistory : jobHistories) {
                assertNull(jobHistory.getErrorMessage());
            }
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jobHistories = jobDetail.getHistoryJobs();
        if (jobHistories != null) {
            for (JobHistory jobHistory : jobHistories) {
                assertNull(jobHistory.getErrorMessage());
            }
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "triggerJob", "addJob"})
    public void deleteJob() {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        JobKey jobKey = new JobKey(JOB_NAME, JOB_GROUP);
        boolean deleted = false;
        try {
            deleted = scheduler.deleteJob(jobKey);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
        assertTrue(deleted);
    }

    private class TriggerJobThread extends Thread {

        private JobKey jobKey;
        TriggerJobThread(JobKey jobKey) {
            super("trigger job thread");
            this.jobKey = jobKey;
            start();
        }

        public void run() {
            try {
                Thread.sleep(1000);
                scheduler.triggerJob(jobKey);
            } catch (SchedulerException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

