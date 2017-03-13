package com.latticeengines.quartz.service;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.domain.exposed.quartz.JobInfoDetail;
import com.latticeengines.quartz.entitymanager.SchedulerEntityMgr;
import com.latticeengines.quartzclient.entitymanager.JobHistoryEntityMgr;

@ContextConfiguration(locations = { "classpath:test-quartz-context.xml" })
public class CustomQuartzJobTestNG extends AbstractTestNGSpringContextTests {

    public static final String JOB_NAME = "testCustomQuartzJob";
    public static final String JOB_GROUP = "CustomQuartzJobs";
    @Autowired
    private Scheduler scheduler;

    @Autowired
    private SchedulerEntityMgr schedulerEntityMgr;

    @Autowired
    private JobHistoryEntityMgr jobHistoryEntityMgr;

    @Value("${quartz.test.functional.testjob.primary.url:}")
    private String testJobPrimaryUrl;

    @Value("${quartz.test.functional.testjob.secondary.url:}")
    private String testJobSecondaryUrl;

    @Value("${quartz.test.functional.testjob.query.api:}")
    private String testJobQueryApi;

    @Value("${quartz.test.functional.testjob.check.bean.url:}")
    private String testJobCheckBeanUrl;

    @Value("${quartz.test.functional.testjob.cron:}")
    private String testJobCronTrigger;

    @BeforeClass
    @Test(groups = "functional")
    public void init() {
        jobHistoryEntityMgr.deleteAllJobHistory(JOB_GROUP, JOB_NAME);
    }

    @Test(groups = "functional")
    public void addJob() {
        schedulerEntityMgr.deleteJob(JOB_GROUP, JOB_NAME);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobName(JOB_NAME);
        jobConfig.setCronTrigger(testJobCronTrigger);
        jobConfig.setDestUrl(testJobPrimaryUrl);
        jobConfig.setSecondaryDestUrl(testJobSecondaryUrl);
        jobConfig.setJobTimeout(30);
        jobConfig.setQueryApi(testJobQueryApi);
        jobConfig.setCheckJobBeanUrl(testJobCheckBeanUrl);
        jobConfig.setJobArguments("{" + "  \"jobType\": \"testQuartzJob\"," + "  \"printMsg\": \"Hello World\"," + "}");
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
        for (int i = 0; i < 5; i++) {
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

    @Test(groups = "functional", dependsOnMethods = { "triggerJob", "addJob" })
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
