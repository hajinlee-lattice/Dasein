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
        // replace userToken and solutionInstanceId before running this test
        String userToken = "e5cea19c5b914319a1fd2bb2fe6f911664b33c6bceef46ecb02d1cd9da88964c";
        String solutionInstanceId = "90ba4b3f-7e65-4661-b6d6-320979df9c17";
        trayService.removeSolutionInstance(
                new TraySettingsBuilder().userToken(userToken).solutionInstanceId(solutionInstanceId).build());
    }

    @Test(groups = "manual", enabled = true)
    public void testGetTrayUserToken() {
        // This tray user ID is for tenant "QA_CDL_Auto_Playbook_Test"
        String trayUserId = "f8b9d93d-f1d7-4076-a4a4-880887b2d2e5";
        trayService.getTrayUserToken(trayUserId);
    }

    @Test(groups = "manual", enabled = true)
    public void testRemoveAuthenticationById() {
        // replace trayAuthId and userToken before running this test
        String trayAuthId = "3fe59cca-ad46-43fa-96fa-2181cf29b1a6";
        String userToken = "e5cea19c5b914319a1fd2bb2fe6f911664b33c6bceef46ecb02d1cd9da88964c";
        trayService.removeAuthenticationById(trayAuthId, userToken);
    }
}
