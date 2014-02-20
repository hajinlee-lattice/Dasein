package com.latticeengines.dataplatform.runtime.metric;

import java.io.File;

import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AnalyticJobMetricsMgrUnitTestNG {
    
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        new File("/tmp/ledpjob-metrics.out").delete();
    }
    
    @Test(groups = "unit")
    public void testMetrics() throws Exception {
        AnalyticJobMetricsMgr mgr = Mockito.spy(AnalyticJobMetricsMgr.getInstance("app_xyz_01"));
        Mockito.when(mgr.getContainerId()).thenReturn("app_xyz_c1_01");
        Mockito.when(mgr.getAppStartTime()).thenReturn(1L);
        Mockito.when(mgr.getContainerLaunchTime()).thenReturn(3001L);

        mgr.initialize();
        Thread.sleep(15000L);
        mgr.finalize();
    }
}
