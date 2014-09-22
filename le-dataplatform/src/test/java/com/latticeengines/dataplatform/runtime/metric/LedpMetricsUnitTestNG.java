package com.latticeengines.dataplatform.runtime.metric;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.perf.exposed.test.PerfFunctionalTestBase;

public class LedpMetricsUnitTestNG {

    private PerfFunctionalTestBase testBase = null;
    private static final String METRICFILE = System.getProperty("user.home") + "/ledpjob-metrics.out";

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        FileUtils.deleteQuietly(new File(METRICFILE));
        testBase = new PerfFunctionalTestBase(METRICFILE);
        testBase.beforeClass();
    }

    @BeforeMethod(groups = "unit")
    public void beforeMethod() {
        testBase.beforeMethod();
    }

    @AfterMethod(groups = "unit")
    public void afterMethod() {
        testBase.afterMethod();
        testBase.flushToFile();
    }

    @Test(groups = "unit")
    public void getMetrics() throws Exception {
        MetricsSystem ms = DefaultMetricsSystem.instance();
        final LedpMetrics lm = LedpMetrics.getForTags(ms,
                Arrays.<MetricsInfo> asList(new MetricsInfo[] { LedpMetricsInfo.Queue }));
        lm.setTagValue(LedpMetricsInfo.Queue, "Priority0.0");
        lm.buildTags();
        lm.start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                lm.incrementNumContainerPreemptions();
                lm.setApplicationCleanupTime(10000L);
                lm.setApplicationWaitTime(3000L);
                lm.setContainerWaitTime(4000L);
                lm.setContainerElapsedTime(5000L);
            }
        }).start();

        Thread.sleep(10000L);
        testBase.flushToFile();
        String contents = FileUtils.readFileToString(new File(METRICFILE));
        assertTrue(contents.contains("Queue=Priority0.0"));
        assertTrue(contents.contains("ContainerWaitTime=4000"));
        assertTrue(contents.contains("ContainerElapsedTime=5000"));
        assertTrue(contents.contains("ApplicationWaitTime=3000"));
        assertTrue(contents.contains("ApplicationCleanupTime=10000"));
        assertTrue(contents.contains("NumContainerPreemptions=1"));

    }
    
    @AfterClass(groups = "unit")
    public void afterClass() {
        testBase.afterClass();
    }
}
