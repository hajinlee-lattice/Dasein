package com.latticeengines.monitor.alerts.service.impl;

import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.monitor.alerts.service.impl.JiraServiceImpl;

public class JiraServiceTestNG {

    @Autowired
    JiraServiceImpl JiraService;

    @Test(groups = "functional", enabled = true)
    public void testTriggerOneDetail() {
        boolean flag = true;
        try {
            JiraService.triggerEvent("JiraServiceTestNG", "http://JiraServiceTestNG", new BasicNameValuePair(
                    "testmetric", "testvalue"));
        } catch (LedpException e) {
            flag = false;
        }
        JiraTestUtils.confirmJiraIncident(flag);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerNoDetail() {
        boolean flag = true;
        try {
            JiraService.triggerEvent("JiraServiceTestNG", "http://JiraServiceTestNG");
        } catch (LedpException e) {
            flag = false;
        }
        JiraTestUtils.confirmJiraIncident(flag);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerMultipleDetail() {
        boolean flag = true;
        try {
            JiraService.triggerEvent("JiraServiceTestNG", "http://JiraServiceTestNG", new BasicNameValuePair(
                    "testmetric", "testvalue"), new BasicNameValuePair("anothertestmetric", "anothertestvalue"));
        } catch (LedpException e) {
            flag = false;
        }
        JiraTestUtils.confirmJiraIncident(flag);
    }

}
