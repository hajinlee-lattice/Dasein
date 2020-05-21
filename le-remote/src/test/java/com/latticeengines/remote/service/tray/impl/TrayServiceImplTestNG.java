package com.latticeengines.remote.service.tray.impl;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.remote.tray.TraySettings.TraySettingsBuilder;
import com.latticeengines.remote.exposed.service.tray.TrayService;

@ContextConfiguration(locations = { "classpath:test-remote-context.xml" })
public class TrayServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private TrayService trayService;

    @Test(groups = "manual", enabled = true)
    public void testRemoveSolutionInstance() {
        String userToken = "dbc3bcbdff634741af8724813c47f178cbd62071dd014b4a8aef3b15f1913d21";
        String solutionInstanceId = "9b54bfd2-1b70-4762-9000-b3e1f7dea2a7";
        trayService.removeSolutionInstance(
                new TraySettingsBuilder().userToken(userToken).solutionInstanceId(solutionInstanceId).build());
    }

}
