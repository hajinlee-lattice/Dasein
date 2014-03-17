package com.latticeengines.dataplatform.exposed.service.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.Test;

public class YarnServiceImplUnitTestNG {

    @Test(groups = "unit")
    public void getPreemptedApps() {
        YarnServiceImpl yarnService = new YarnServiceImpl();
        RestTemplate rmRestTemplate = mock(RestTemplate.class);
        Configuration yarnConfiguration = mock(Configuration.class);
        ReflectionTestUtils.setField(yarnService, "rmRestTemplate", rmRestTemplate);
        ReflectionTestUtils.setField(yarnService, "yarnConfiguration", yarnConfiguration);

        when(yarnConfiguration.get("yarn.resourcemanager.webapp.address")).thenReturn("localhost:8088");

        AppInfo app1 = mock(AppInfo.class);
        AppInfo app2 = mock(AppInfo.class);
        AppInfo app3 = mock(AppInfo.class);
        AppInfo app4 = mock(AppInfo.class);

        when(app1.getStartTime()).thenReturn(1L);
        when(app2.getStartTime()).thenReturn(2L);
        when(app3.getStartTime()).thenReturn(3L);
        when(app4.getStartTime()).thenReturn(4L);

        when(app1.getQueue()).thenReturn("root.Priority0.A");
        when(app2.getQueue()).thenReturn("root.Priority1.A");
        when(app3.getQueue()).thenReturn("root.Priority0.B");
        when(app4.getQueue()).thenReturn("root.Priority1.B");

        when(app1.getNote()).thenReturn("-102 Container preempted by scheduler");
        when(app2.getNote()).thenReturn("-102 Container preempted by scheduler");
        when(app3.getNote()).thenReturn("-102 Container preempted by scheduler");
        when(app4.getNote()).thenReturn("-102 Container preempted by scheduler");

        AppsInfo apps = mock(AppsInfo.class);
        ArrayList<AppInfo> list = new ArrayList<AppInfo>();
        list.add(app1);
        list.add(app2);
        list.add(app3);
        list.add(app4);
        when(apps.getApps()).thenReturn(list);

        when(rmRestTemplate.getForObject("http://localhost:8088/ws/v1/cluster/apps?states=FAILED", AppsInfo.class))
                .thenReturn(apps);

        List<AppInfo> sortedApps = yarnService.getPreemptedApps();

        assertEquals(app1.getQueue(), sortedApps.get(0).getQueue());
        assertEquals(app3.getQueue(), sortedApps.get(1).getQueue());
        assertEquals(app2.getQueue(), sortedApps.get(2).getQueue());
        assertEquals(app4.getQueue(), sortedApps.get(3).getQueue());
    }
}
