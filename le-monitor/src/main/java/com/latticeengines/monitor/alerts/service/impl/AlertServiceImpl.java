package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.monitor.exposed.alerts.service.AlertService;
import com.latticeengines.monitor.exposed.alerts.service.PagerDutyService;

@Component("alertService")
@Scope("prototype")
public class AlertServiceImpl implements AlertService {

    private static final Log log = LogFactory.getLog(AlertServiceImpl.class);

    @Value("${monitor.alert.service.enabled:false}")
    private boolean alertServiceEnabled;

    @Autowired
    private PagerDutyService pagerDutyService;

    @Override
    public String triggerCriticalEvent(String description, String clientUrl, BasicNameValuePair... details) {
        return this.triggerCriticalEvent(description, clientUrl, Arrays.asList(details));
    }

    @Override
    public String triggerCriticalEvent(String description, String clientUrl,
            Iterable<? extends BasicNameValuePair> details) {
        if (!this.alertServiceEnabled) {
            return "";
        }

        String result = "";

        try {
            result = this.pagerDutyService.triggerEvent(description, clientUrl, details);
        } catch (IOException e) {
            // Intentionally log and consume error
            log.error("Problem sending event to PagerDuty", e);
        }

        return result;
    }

    public void enableTestMode() {
        this.alertServiceEnabled = true;
        ((PagerDutyServiceImpl) this.pagerDutyService).useTestServiceApiKey();
    }
}
