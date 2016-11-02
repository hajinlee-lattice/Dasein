package com.latticeengines.quartz.service;

import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.domain.exposed.quartz.JobInfoDetail;
import com.latticeengines.quartz.entitymanager.SchedulerEntityMgr;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@ContextConfiguration(locations = { "classpath:test-quartz-context.xml" })
public class CustomQuartzJobTestNG extends AbstractTestNGSpringContextTests {

    public static final String JOB_NAME = "testCustomQuartzJob";
    public static final String JOB_GROUP = "CustomQuartzJobs";
    @Autowired
    private Scheduler scheduler;

    @Autowired
    private SchedulerEntityMgr schedulerEntityMgr;

    @Autowired
    private ApplicationContext appContext;

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void addJob() {
        schedulerEntityMgr.deleteJob(JOB_GROUP, JOB_NAME);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobName(JOB_NAME);
        jobConfig.setCronTrigger("0/5 * * * * ?");
        jobConfig.setDestUrl("http://localhost:8899/quartz/quartzjob/triggerjob");
        jobConfig.setSecondaryDestUrl("http://localhost:8899/quartz/quartzjob/triggerjob");
        jobConfig.setJobTimeout(30);
        jobConfig.setQueryApi("http://localhost:8899/quartz/quartzjob/checkactivejob");
        jobConfig.setCheckJobBeanUrl("http://localhost:8899/quartz/quartzjob/checkjobbean");
        jobConfig.setJobArguments("{" +
                "  \"jobType\": \"testQuartzJob\"," +
                "  \"printMsg\": \"Hello World\"," + "}");
        schedulerEntityMgr.addJob(JOB_GROUP, jobConfig);
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
        Boolean deleted = schedulerEntityMgr.deleteJob(JOB_GROUP, JOB_NAME);
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
            } catch (SchedulerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
