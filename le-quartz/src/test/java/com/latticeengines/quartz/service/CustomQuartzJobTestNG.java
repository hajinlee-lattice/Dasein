package com.latticeengines.quartz.service;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.domain.exposed.quartz.JobInfoDetail;
import com.latticeengines.quartz.entitymanager.SchedulerEntityMgr;
import com.latticeengines.quartzclient.entitymanager.JobHistoryEntityMgr;

@ContextConfiguration(locations = { "classpath:test-quartz-context.xml" })
public class CustomQuartzJobTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(CustomQuartzJobTestNG.class);

    public static final String JOB_NAME = "testCustomQuartzJob";
    public static final String JOB_GROUP = "CustomQuartzJobs";
    @Inject
    private Scheduler scheduler;

    @Inject
    private SchedulerEntityMgr schedulerEntityMgr;

    @Inject
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
        jobConfig.setJobArguments("{" + "  \"jobType\": \"testQuartzJob\"," + "  \"printMsg\": \"Hello World\"" + "}");
        schedulerEntityMgr.addJob(JOB_GROUP, jobConfig);
    }

    @Test(groups = "functional", dependsOnMethods = { "addJob" })
    public void getJobList() {
        List<JobInfo> jobInfos = schedulerEntityMgr.listJobs(JOB_GROUP);
        assertTrue(jobInfos.size() >= 1);
        SleepUtils.sleep(10000);
    }

    @Test(groups = "functional", dependsOnMethods = { "addJob" })
    public void triggerJob() {
        JobKey jobKey = new JobKey(JOB_NAME, JOB_GROUP);
        try {
            scheduler.triggerJob(jobKey);
        } catch (SchedulerException e) {
            log.error(String.format("Failed to trigger job %s.", jobKey), e);
        }
        List<TriggerJobThread> jobList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            jobList.add(new TriggerJobThread(jobKey));
            SleepUtils.sleep(100);
        }
        JobInfoDetail jobDetail = schedulerEntityMgr.getJobDetail(JOB_GROUP, JOB_NAME);
        List<JobHistory> jobHistories = jobDetail.getHistoryJobs();
        if (jobHistories != null) {
            for (JobHistory jobHistory : jobHistories) {
                assertNull(jobHistory.getErrorMessage());
            }
        }
        SleepUtils.sleep(5000);
        jobHistories = jobDetail.getHistoryJobs();
        if (jobHistories != null) {
            for (JobHistory jobHistory : jobHistories) {
                assertNull(jobHistory.getErrorMessage());
            }
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "triggerJob", "addJob" })
    public void deleteJob() {
        SleepUtils.sleep(10000);
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
            } catch (Exception e) {
                log.error(String.format("Failed to trigger job %s.", jobKey), e);
            }
        }
    }
}
