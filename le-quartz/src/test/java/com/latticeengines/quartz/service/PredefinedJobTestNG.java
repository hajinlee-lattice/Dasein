package com.latticeengines.quartz.service;

import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.quartz.entitymanager.SchedulerEntityMgr;

@ContextConfiguration(locations = { "classpath:test-quartz-context.xml" })
public class PredefinedJobTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private SchedulerEntityMgr schedulerEntityMgr;

    @Autowired
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
        List<JobInfo> jobInfos = schedulerEntityMgr.listJobs("PredefinedJobs");
        assertTrue(jobInfos.size() >= 1);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "getJobList" })
    public void deleteJob() {
        Boolean deleted = schedulerEntityMgr.deleteJob("PredefinedJobs", "testPredefinedJob");
        assertTrue(deleted);
    }
}
