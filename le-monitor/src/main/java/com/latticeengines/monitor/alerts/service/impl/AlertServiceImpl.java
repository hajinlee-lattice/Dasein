package com.latticeengines.monitor.alerts.service.impl;

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
    private PagerDutyService pagerDutyEmailService;

    @Override
    public String triggerCriticalEvent(String description, String clientUrl, String dedupKey,
            BasicNameValuePair... details) {
        return triggerCriticalEvent(description, clientUrl, dedupKey, Arrays.asList(details));
    }

    @Override
    public String triggerCriticalEvent(String description, String clientUrl, String dedupKey,
            Iterable<? extends BasicNameValuePair> details) {
        if (!this.alertServiceEnabled) {
            return "disabled";
        }

        try {
            pagerDutyEmailService.triggerEvent(description, clientUrl, dedupKey, details);
        } catch (Exception e) {
            // Intentionally log and consume error
            log.fatal(String.format("Problem sending event to PagerDuty. description:%s clientUrl:%s dedupKey:%s",
                    description, clientUrl, dedupKey), e);
            return "fail";
        }
        return "success";
    }

    public void enableTestMode() {
        this.alertServiceEnabled = true;
        ((PagerDutyEmailServiceImpl) this.pagerDutyEmailService).useTestService();
    }
}
