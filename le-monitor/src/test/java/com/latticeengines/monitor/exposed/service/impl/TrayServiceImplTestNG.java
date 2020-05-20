package com.latticeengines.monitor.exposed.service.impl;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.monitor.TraySettings.TraySettingsBuilder;
import com.latticeengines.monitor.exposed.service.TrayService;

@ContextConfiguration(locations = { "classpath:test-monitor-context.xml" })
public class TrayServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private TrayService trayService;

    @Test(groups = "manual", enabled = false)
    public void testRemoveeSolutionInstance() {
        String userToken = "dbc3bcbdff634741af8724813c47f178cbd62071dd014b4a8aef3b15f1913d21";
        String solutionInstanceId = "870615d2-ba7b-4aa1-8c50-fc44f6a443af";
        trayService.removeSolutionInstance(
                new TraySettingsBuilder().userToken(userToken).solutionInstanceId(solutionInstanceId).build());
    }

}
