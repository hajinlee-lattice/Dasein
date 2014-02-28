package com.latticeengines.dataplatform.runtime.metric;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.perf.exposed.test.PerfFunctionalTestBase;

public class LedpMetricsMgrUnitTestNG {

    private static final String METRICFILE = "/tmp/ledpjob-metrics.out";
    private PerfFunctionalTestBase testBase = null;

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
    public void start() throws Exception {
        MetricsSystem ms = DefaultMetricsSystem.instance();
        final LedpMetrics lm1 = LedpMetrics.getForTags(ms,
                Arrays.<MetricsInfo> asList(new MetricsInfo[] { LedpMetricsInfo.Priority }));
        final LedpMetrics lm2 = LedpMetrics.getForTags(ms,
                Arrays.<MetricsInfo> asList(new MetricsInfo[] { LedpMetricsInfo.Queue }));
        lm1.setTagValue(LedpMetricsInfo.Priority, "0");
        lm2.setTagValue(LedpMetricsInfo.Queue, "Priority0.A");

        List<LedpMetrics> lm = new ArrayList<LedpMetrics>();
        lm.add(lm1);
        lm.add(lm2);
        final LedpMetricsMgr mgr = new LedpMetricsMgr();
        ReflectionTestUtils.setField(mgr, "ledpMetrics", lm);

        mgr.start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                mgr.setAppStartTime(1000L);
                mgr.setAppSubmissionTime(200L);
                mgr.setAppEndTime(11000L);
                mgr.setContainerLaunchTime(3000L);
                mgr.incrementNumberPreemptions();
                
                Assert.assertEquals(10800, lm1.applicationElapsedTime.value());
            }
        }).start();

        Thread.sleep(10000L);
        testBase.flushToFile();
        String contents = FileUtils.readFileToString(new File(METRICFILE));
        Assert.assertTrue(contents.contains("Priority=0"));
        Assert.assertTrue(contents.contains("Queue=Priority0.A"));
        Assert.assertTrue(contents.contains("ContainerWaitTime=2000"));
        Assert.assertTrue(contents.contains("ApplicationWaitTime=800"));
        Assert.assertTrue(contents.contains("ApplicationElapsedTime=10800"));
        Assert.assertTrue(contents.contains("NumContainerPreemptions=1"));
        
    }
}
