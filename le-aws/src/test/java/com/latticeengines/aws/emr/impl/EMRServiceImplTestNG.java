package com.latticeengines.aws.emr.impl;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupStatus;
import com.latticeengines.aws.emr.EMRService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class EMRServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(EMRServiceImplTestNG.class);

    @Value("${aws.test.emr.cluster}")
    private String clusterName;

    @Autowired
    private EMRService emrService;

    @BeforeClass(groups = "functional")
    private void setup() {
    }

    @AfterClass(groups = "functional")
    private void teardown() {
    }

    @Test(groups = "functional")
    public void testExtractConfiguration() {
        String masterIp = emrService.getMasterIp(clusterName);
        Assert.assertTrue(StringUtils.isNotBlank(masterIp));
    }

    @Test(groups = "manual", enabled = false)
    public void testResizing() {
        String clusterName = "emr_cluster";
        emrService.scaleTaskGroup(clusterName, 2);
        long start = System.currentTimeMillis();
        InstanceGroup taskGrp = emrService.getTaskGroup(clusterName);
        int requested = taskGrp.getRequestedInstanceCount();
        int running = taskGrp.getRunningInstanceCount();
        InstanceGroupStatus status = taskGrp.getStatus();
        while (requested != running) {
            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                log.info("Interrupted");
            }
            long duration = System.currentTimeMillis() - start;
            log.info(String.format("Waiting for resizing to finish, %d/%d, Status=%s, TimeElapsed=%.1f sec", //
                    running, requested, status, duration / 1000.0D));
            taskGrp = emrService.getTaskGroup(clusterName);
            requested = taskGrp.getRequestedInstanceCount();
            running = taskGrp.getRunningInstanceCount();
            status = taskGrp.getStatus();
        }
    }

}
