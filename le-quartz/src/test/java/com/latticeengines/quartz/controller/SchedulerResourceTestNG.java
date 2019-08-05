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
public class SchedulerResourceTestNG extends AbstractTestNGSpringContextTests {

    @Value("${quartz.test.functional.testdesturl}")
    private String testDestUrl;

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

    @Autowired
    private QuartzSchedulerProxy quartzSchedulerProxy;

    @Test(groups = "functional", dependsOnMethods = { "addJob", "addRepeatJob" }, enabled = false)
    public void getJobList() {
        List<JobInfo> jobInfos = quartzSchedulerProxy.listJobs("groupTestNG");
        assertTrue(jobInfos.size() >= 1);
    }

    @Test(groups = "functional", enabled = false)
    public void addJob() {
        quartzSchedulerProxy.deleteJob("groupTestNG", "testNGJob");
        JobConfig jobTest = new JobConfig();
        jobTest.setJobName("testNGJob");
        jobTest.setCronTrigger(testJobCronTrigger);
        jobTest.setDestUrl(testJobPrimaryUrl);
        jobTest.setSecondaryDestUrl(testJobSecondaryUrl);
        jobTest.setJobTimeout(30);
        jobTest.setQueryApi(testJobQueryApi);
        jobTest.setCheckJobBeanUrl(testJobCheckBeanUrl);
        jobTest.setJobArguments("{" +
                "  \"jobType\": \"testQuartzJob\"," +
                "  \"printMsg\": \"Hello World\"" + "}");
        Boolean success = quartzSchedulerProxy.addJob("groupTestNG", jobTest);
        assertEquals(success, Boolean.TRUE);
    }

    @Test(groups = "functional", dependsOnMethods = { "addJob" }, enabled = false)
    public void getJobDetails() {
        JobInfoDetail jobInfoDetail = quartzSchedulerProxy.getJobDetail("groupTestNG",
                "testNGJob");
        assertNotNull(jobInfoDetail);
        assertEquals(jobInfoDetail.getJobName(), "testNGJob");
    }

    @Test(groups = "functional", dependsOnMethods = { "addJob" }, enabled = false)
    public void addRepeatJob() {
        JobConfig jobTest = new JobConfig();
        jobTest.setCronTrigger("0/5 * * * * ?");
        jobTest.setJobName("testNGJob");
        jobTest.setDestUrl(testDestUrl);
        Boolean success = quartzSchedulerProxy.addJob("groupTestNG", jobTest);
        assertEquals(success, Boolean.FALSE);
    }

    @Test(groups = "functional", expectedExceptions = Exception.class, enabled = false)
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
        Boolean success = quartzSchedulerProxy.addJob("groupTestNG", jobTest);
        assertEquals(success, Boolean.FALSE);
    }

    @Test(groups = "functional", dependsOnMethods = { "getJobList" }, enabled = false)
    public void deleteJob() {
        Boolean success = quartzSchedulerProxy.deleteJob("groupTestNG", "testNGJob");
        assertEquals(success, Boolean.TRUE);
    }
}
