package com.latticeengines.camille;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.JobPublisher;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleEnvironment.Mode;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.netflix.curator.test.TestingServer;

import java.util.UUID;

public class JobPublisherUnitTestNG {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testSimpleJobCreate() throws  Exception
    {

        String path = "/JobTest";
        String jobID = "Job1";
        JobPublisher publisher = new JobPublisher(path);
        publisher.registerJob(jobID, "unusedData");

        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                new Path("/JobTest/Requests/Job1")));

        CamilleEnvironment.getCamille().delete(new Path("/JobTest/Requests/Job1"));

    }

    @Test(groups = "unit")
    public void testTryNum() throws  Exception
    {
        String path = "/JobTest";
        String jobID = "Job2";
        JobPublisher publisher = new JobPublisher(path);
        publisher.registerJob(jobID, "unusedData");

        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                new Path("/JobTest/Requests/Job2/TryNum")));

        CamilleEnvironment.getCamille().delete(new Path("/JobTest/Requests/Job2"));

    }

    @Test(groups = "unit")
    public void testJobIsOpen() throws  Exception
    {
        String path = "/JobTest";
        String jobID = "Job4";
        JobPublisher publisher = new JobPublisher(path);
        publisher.registerJob(jobID, "unusedData");

        Assert.assertTrue(publisher.isOpenJob(jobID));

        CamilleEnvironment.getCamille().delete(new Path("/JobTest/Requests/Job4"));

    }

    public void executorExample(String nodeData)
    {
        JobPublisher publisher = new JobPublisher("/JobTest");
        Assert.assertFalse(publisher.isOpenJob(nodeData));
    }

    @Test(groups = "unit")
    public void testExecutor() throws  Exception
    {
        String path = "/JobTest";
        String jobID = "Job_5";
        JobPublisher publisher = new JobPublisher(path);
        publisher.registerJob(jobID, jobID);

        JobPublisher.IJobExecutor executor = (s)->{executorExample(s);};

        Assert.assertTrue(
                publisher.attemptExecution(executor, "testExecutor", jobID));

        Path jobPath = publisher.getRequestPath(jobID);
        if(CamilleEnvironment.getCamille().exists(jobPath))
            CamilleEnvironment.getCamille().delete(jobPath);
    }

    @Test(groups = "unit")
    public void testRepeatedFailure() throws  Exception {
        String path = "/JobTest";
        String jobID = "Job_7";
        JobPublisher publisher = new JobPublisher(path);
        Path failurePath = new Path("/JobTest/Failures/Job_7");

        try {


            publisher.registerJob(jobID, jobID);

            JobPublisher.IJobExecutor executor = (s) -> {
                log.info("stupid exception generator" + Integer.toString(1 / 0));
            };

            Assert.assertFalse(
                    publisher.attemptExecution(executor, "testExecutor", jobID));


            Assert.assertFalse(
                    publisher.attemptExecution(executor, "testExecutor", jobID));

            Assert.assertFalse(
                    publisher.attemptExecution(executor, "testExecutor", jobID));

            Assert.assertTrue(CamilleEnvironment.getCamille().exists(failurePath));
        } finally {


            if (CamilleEnvironment.getCamille().exists(failurePath))
                CamilleEnvironment.getCamille().delete(failurePath);
        }
    }



}
