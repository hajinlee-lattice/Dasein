package com.latticeengines.dataplatform.runtime.metric;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class LedpMetricsUnitTestNG {

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        new File("/tmp/ledpjob-metrics.out").delete();
    }

    @Test(groups = "unit")
    public void getMetrics() throws Exception {
        MetricsSystem ms = DefaultMetricsSystem.instance();
        final LedpMetrics lm = LedpMetrics.getForTags(ms, Arrays.<MetricsInfo>asList(new MetricsInfo [] { LedpMetricsInfo.Queue }));
        lm.setTagValue(LedpMetricsInfo.Queue, "Priority0.A");
        
        lm.start();
        
        new Thread(new Runnable() {
            @Override
            public void run() {
                lm.incrementNumContainerPreemptions();
                lm.setApplicationElapsedTime(10000L);
                lm.setApplicationWaitTime(3000L);
                lm.setContainerWaitTime(4000L);
            }
        }).start();
        
        Thread.sleep(10000L);

        String contents = FileUtils.readFileToString(new File("/tmp/ledpjob-metrics.out"));
        Assert.assertTrue(contents.contains("Queue=Priority0.A"));
        Assert.assertTrue(contents.contains("ContainerWaitTime=400"));
        Assert.assertTrue(contents.contains("ApplicationWaitTime=3000"));
        Assert.assertTrue(contents.contains("ApplicationElapsedTime=10000"));
        Assert.assertTrue(contents.contains("NumContainerPreemptions=1"));

    }
}
