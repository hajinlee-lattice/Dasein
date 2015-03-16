package com.latticeengines.monitor.exposed.service.impl;

import java.util.Arrays;

import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.monitor.exposed.service.AlertService;


@Component("alertService")
public class AlertServiceImpl implements AlertService {


    @Value("${monitor.alertService.enabled}")
    private boolean alertServiceEnabled;

    @Autowired
    private PagerDutyServiceImpl pagerDutyService;
    
    @Autowired
    private JiraServiceImpl jiraService;

    public void triggerCriticalEvent(String description, String clientUrl, BasicNameValuePair... details) {
        triggerCriticalEvent(description, clientUrl, Arrays.asList(details));
    }

    public void triggerCriticalEvent(String description, String clientUrl, Iterable<? extends BasicNameValuePair> details) {
        if (!alertServiceEnabled) {
            return;
        }

        pagerDutyService.triggerEvent(description, clientUrl, details);
        jiraService.triggerEvent(description, clientUrl, details);
    }

    public void enableTestMode() {
        alertServiceEnabled = true;
        pagerDutyService.useTestServiceApiKey();
    }
}
