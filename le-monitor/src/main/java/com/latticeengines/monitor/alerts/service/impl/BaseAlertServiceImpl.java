package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.monitor.exposed.alerts.service.AlertService;


public class BaseAlertServiceImpl implements AlertService {

    private static final Log log = LogFactory.getLog(BaseAlertServiceImpl.class);

    @Value("${monitor.alertService.enabled}")
    private boolean alertServiceEnabled;

    protected BasePagerDutyServiceImpl pagerDutyService;

    public String triggerCriticalEvent(String description, String clientUrl, BasicNameValuePair... details) {
        return triggerCriticalEvent(description, clientUrl, Arrays.asList(details));
    }

    public String triggerCriticalEvent(String description, String clientUrl, Iterable<? extends BasicNameValuePair> details) {
        if (!alertServiceEnabled) {
            return "";
        }

        String result = "";

        try {
            result = pagerDutyService.triggerEvent(description, clientUrl, details);
        } catch (IOException e) {
            // Intentionally log and consume error
            log.error("Problem sending event to PagerDuty", e);
        }

        return result;
    }

    public void enableTestMode() {
        alertServiceEnabled = true;
        pagerDutyService.useTestServiceApiKey();
    }
}
