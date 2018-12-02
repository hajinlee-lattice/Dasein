package com.latticeengines.yarn.exposed.service.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

public class YarnServiceImplUnitTestNG {

    @Test(groups = "unit")
    public void getPreemptedApps() {
        YarnServiceImpl yarnService = new YarnServiceImpl();
        YarnClient yarnClient = mock(YarnClient.class);
        ReflectionTestUtils.setField(yarnService, "yarnClient", yarnClient);

        ApplicationReport app1 = mock(ApplicationReport.class);
        ApplicationReport app2 = mock(ApplicationReport.class);
        ApplicationReport app3 = mock(ApplicationReport.class);
        ApplicationReport app4 = mock(ApplicationReport.class);
        ApplicationReport app5 = mock(ApplicationReport.class);

        when(app1.getStartTime()).thenReturn(1L);
        when(app2.getStartTime()).thenReturn(2L);
        when(app3.getStartTime()).thenReturn(3L);
        when(app4.getStartTime()).thenReturn(4L);
        when(app5.getStartTime()).thenReturn(5L);

        when(app1.getQueue()).thenReturn("root.Priority0.0");
        when(app2.getQueue()).thenReturn("root.Priority1.0");
        when(app3.getQueue()).thenReturn("root.Priority0.1");
        when(app4.getQueue()).thenReturn("root.Priority1.1");
        when(app5.getQueue()).thenReturn("default");

        when(app1.getDiagnostics()).thenReturn("-102 Container preempted by scheduler");
        when(app2.getDiagnostics()).thenReturn("-102 Container preempted by scheduler");
        when(app3.getDiagnostics()).thenReturn("-102 Container preempted by scheduler");
        when(app4.getDiagnostics()).thenReturn("-102 Container preempted by scheduler");
        when(app5.getDiagnostics()).thenReturn("-102 Container preempted by scheduler");

        List<ApplicationReport> list = new ArrayList<>();
        list.add(app1);
        list.add(app2);
        list.add(app3);
        list.add(app4);
        list.add(app5);

        try {
            when(yarnClient.getApplications(EnumSet.of(YarnApplicationState.FAILED))).thenReturn(list);
        } catch (IOException| YarnException e) {
            throw new RuntimeException(e);
        }

        List<ApplicationReport> sortedApps = yarnService.getPreemptedApps();
        assertEquals(4, sortedApps.size());

        assertEquals(app1.getQueue(), sortedApps.get(0).getQueue());
        assertEquals(app3.getQueue(), sortedApps.get(1).getQueue());
        assertEquals(app2.getQueue(), sortedApps.get(2).getQueue());
        assertEquals(app4.getQueue(), sortedApps.get(3).getQueue());
    }
}
