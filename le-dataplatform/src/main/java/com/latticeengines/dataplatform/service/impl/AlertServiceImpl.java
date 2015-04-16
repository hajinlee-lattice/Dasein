package com.latticeengines.dataplatform.service.impl;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.service.AlertService;

@Component("alertService")
public class AlertServiceImpl implements AlertService {

    private static final Log log = LogFactory.getLog(AlertServiceImpl.class);

    @Value("${dataplatform.alertService.enabled}")
    private boolean alertServiceEnabled;

    @Autowired
    private PagerDutyServiceImpl pagerDutyService;

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
