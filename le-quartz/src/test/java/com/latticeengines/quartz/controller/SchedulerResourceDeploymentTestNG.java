package com.latticeengines.quartz.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.domain.exposed.quartz.JobInfoDetail;
import com.latticeengines.proxy.exposed.quartz.QuartzSchedulerProxy;

@ContextConfiguration(locations = { "classpath:test-quartz-context.xml" })
public class SchedulerResourceDeploymentTestNG extends AbstractTestNGSpringContextTests {
    @Value("${quartz.test.deployment.testjob.primary.url:}")
    private String testJobPrimaryUrl;

    @Value("${quartz.test.deployment.testjob.secondary.url:}")
    private String testJobSecondaryUrl;

    @Value("${quartz.test.deployment.testjob.query.api:}")
    private String testJobQueryApi;

    @Value("${quartz.test.deployment.testjob.check.bean.url:}")
    private String testJobCheckBeanUrl;

    @Value("${quartz.test.deployment.testjob.cron:}")
    private String testJobCronTrigger;

    @Autowired
    private QuartzSchedulerProxy quartzSchedulerProxy;

    private static final String JOB_NAME = "modelSummaryFullDownload";

    private static final String TENANT_ID = "PredefinedJobs";

    @Test(groups = "deployment")
    public void getJobList() {
        List<JobInfo> jobInfos = quartzSchedulerProxy.listJobs(TENANT_ID);
        assertTrue(jobInfos.size() >= 1);
    }

    @Test(groups = "deployment", dependsOnMethods = { "getJobList" })
    public void listAllJobs() {
        List<JobInfo> jobInfos = quartzSchedulerProxy.listAllJobs();
        assertTrue(jobInfos.size() >= 1);
    }

    @Test(groups = "deployment", dependsOnMethods = { "listAllJobs" })
    public void getJobDetails() {
        JobInfoDetail jobInfoDetail = quartzSchedulerProxy.getJobDetail(TENANT_ID, JOB_NAME);
        assertNotNull(jobInfoDetail);
        assertEquals(jobInfoDetail.getJobName(), JOB_NAME);
    }

    @Test(groups = "deployment", dependsOnMethods = { "getJobDetails" })
    public void setSchedulerStatus() {
        Boolean success = quartzSchedulerProxy.setSchedulerStatus("Other");
        assertEquals(success, Boolean.FALSE);
    }

    @Test(groups = "deployment", dependsOnMethods = { "setSchedulerStatus" })
    public void deleteJob() {
        Boolean success = quartzSchedulerProxy.deleteJob(TENANT_ID, JOB_NAME);
        assertEquals(success, Boolean.TRUE);
    }

    @Test(groups = "deployment", dependsOnMethods = { "deleteJob" })
    public void addJob() {
        quartzSchedulerProxy.deleteJob(TENANT_ID, JOB_NAME);
        JobConfig jobTest = new JobConfig();
        jobTest.setJobName(JOB_NAME);
        jobTest.setCronTrigger(testJobCronTrigger);
        jobTest.setDestUrl(testJobPrimaryUrl);
        jobTest.setSecondaryDestUrl(testJobSecondaryUrl);
        jobTest.setJobTimeout(30);
        jobTest.setQueryApi(testJobQueryApi);
        jobTest.setCheckJobBeanUrl(testJobCheckBeanUrl);
        jobTest.setJobArguments("{\"jobType\": \"modelSummaryFullDownload\"}");
        Boolean success = quartzSchedulerProxy.addJob(TENANT_ID, jobTest);
        assertEquals(success, Boolean.TRUE);
    }

    @Test(groups = "deployment", dependsOnMethods = { "addJob" })
    public void addRepeatJob() {
        JobConfig jobTest = new JobConfig();
        jobTest.setCronTrigger("0/5 * * * * ?");
        jobTest.setJobName(JOB_NAME);
        jobTest.setDestUrl(testJobPrimaryUrl);
        Boolean success = quartzSchedulerProxy.addJob(TENANT_ID, jobTest);
        assertEquals(success, Boolean.FALSE);
    }

    @Test(groups = "deployment", expectedExceptions = Exception.class)
    public void addWrongDestUrlJob() {
        JobConfig jobTest = new JobConfig();
        jobTest.setJobName("testNGWrongDestUrlJob");
        jobTest.setCronTrigger(testJobCronTrigger);
        jobTest.setDestUrl("//localhost:8080/quartz/quartzjob/triggerjob");
        jobTest.setSecondaryDestUrl("/localhost:8080/quartz/quartzjob/triggerjob");
        jobTest.setJobTimeout(30);
        jobTest.setQueryApi("//localhost:8080/quartz/quartzjob/checkactivejob");
        jobTest.setCheckJobBeanUrl(testJobCheckBeanUrl);
        jobTest.setJobArguments("{" +
                "  \"jobType\": \"testQuartzJob\"," +
                "  \"printMsg\": \"Hello World\"" + "}");
        Boolean success = quartzSchedulerProxy.addJob(TENANT_ID, jobTest);
        assertEquals(success, Boolean.FALSE);
    }
}
