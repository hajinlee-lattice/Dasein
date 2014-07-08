package com.latticeengines.dataplatform.service.impl.watchdog;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.exposed.service.impl.YarnServiceImpl;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.impl.JobServiceImpl;

public class ThrottleLongHangingJobsUnitTestNG {

    private JobService jobService;

    @BeforeClass(groups = "unit")
    public void beforeClass() throws Exception {
        initMocks(this);
        jobService = mock(JobServiceImpl.class);
        doNothing().when(jobService).killJob((ApplicationId) any());
    }

    @Test(groups = "unit", enabled = true)
    public void testThrottle() {
        int testCase = 10;
        YarnService yarnService = generateYarnService(testCase, 0.5f);
        ThrottleLongHangingJobs throttleLongHangingJobs = setupThrottleLongHangingJobs(yarnService);

        for (int i = 0; i < 5; i++) {
            try {
                throttleLongHangingJobs.run(null);
                Thread.sleep(3000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        verify(jobService, times(testCase)).killJob((ApplicationId) any());
    }

    @Test(groups = "unit")
    public void testRemoveCompletedApps() {
        int testCase = 10;
        YarnService yarnService = generateYarnService(testCase, 0.5f);
        ThrottleLongHangingJobs throttleLongHangingJobs = setupThrottleLongHangingJobs(yarnService);

        try {
            throttleLongHangingJobs.run(null);
            Thread.sleep(3000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 7 apps completed
        yarnService = generateYarnService(3, 0.5f);
        throttleLongHangingJobs.setYarnService(yarnService);
        try {
            throttleLongHangingJobs.run(null);
            Thread.sleep(3000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(jobService, never()).killJob((ApplicationId) any());
    }

    private ThrottleLongHangingJobs setupThrottleLongHangingJobs(YarnService yarnService) {
        ThrottleLongHangingJobs throttleLongHangingJobs = new ThrottleLongHangingJobs();
        ReflectionTestUtils.setField(throttleLongHangingJobs, "throttleThreshold", 9000L);
        throttleLongHangingJobs.setJobService(jobService);
        throttleLongHangingJobs.setYarnService(yarnService);

        return throttleLongHangingJobs;
    }

    private YarnService generateYarnService(int testCase, float progress) {
        YarnService yarnService = mock(YarnServiceImpl.class);
        AppsInfo appsInfo = new AppsInfo();
        String appPrefix = "application_1404235843612_";
        for (int i = 0; i < testCase; i++) {
            AppInfo appInfo = mock(AppInfo.class);
            when(appInfo.getAppId()).thenReturn(appPrefix + i);
            when(appInfo.getProgress()).thenReturn((float) progress);
            appsInfo.add(appInfo);
        }

        when(yarnService.getApplications("states=RUNNING")).thenReturn(appsInfo);

        return yarnService;
    }

}
