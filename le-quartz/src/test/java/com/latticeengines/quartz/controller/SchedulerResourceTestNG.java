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

    @Autowired
    private QuartzSchedulerProxy quartzSchedulerProxy;

    @Test(groups = "functional", dependsOnMethods = { "addJob", "addRepeatJob" })
    public void getJobList() {
        List<JobInfo> jobInfos = quartzSchedulerProxy.listJobs("groupTestNG");
        assertTrue(jobInfos.size() >= 1);
    }

    @Test(groups = "functional")
    public void addJob() {
        JobConfig jobTest = new JobConfig();
        jobTest.setCronTrigger("0/5 * * * * ?");
        jobTest.setJobName("testNGJob");
        jobTest.setDestUrl(testDestUrl);
        Boolean success = quartzSchedulerProxy.addJob("groupTestNG", jobTest);
        assertEquals(success, Boolean.TRUE);
    }

    @Test(groups = "functional", dependsOnMethods = { "addJob" })
    public void getJobDetails() {
        JobInfoDetail jobInfoDetail = quartzSchedulerProxy.getJobDetail("groupTestNG",
                "testNGJob");
        assertNotNull(jobInfoDetail);
        assertEquals(jobInfoDetail.getJobName(), "testNGJob");
    }

    @Test(groups = "functional", dependsOnMethods = { "addJob" })
    public void addRepeatJob() {
        JobConfig jobTest = new JobConfig();
        jobTest.setCronTrigger("0/5 * * * * ?");
        jobTest.setJobName("testNGJob");
        jobTest.setDestUrl(testDestUrl);
        Boolean success = quartzSchedulerProxy.addJob("groupTestNG", jobTest);
        assertEquals(success, Boolean.FALSE);
    }

    @Test(groups = "functional", expectedExceptions = Exception.class)
    public void addWrongDestUrlJob() {
        JobConfig jobTest = new JobConfig();
        jobTest.setCronTrigger("0/5 * * * * ?");
        jobTest.setJobName("testNGWrongDestUrlJob");
        jobTest.setDestUrl("//localhost:8080");
        Boolean success = quartzSchedulerProxy.addJob("groupTestNG", jobTest);
        assertEquals(success, Boolean.FALSE);
    }

    @Test(groups = "functional", dependsOnMethods = { "getJobList" })
    public void deleteJob() {
        Boolean success = quartzSchedulerProxy.deleteJob("groupTestNG", "testNGJob");
        assertEquals(success, Boolean.TRUE);
    }
}
