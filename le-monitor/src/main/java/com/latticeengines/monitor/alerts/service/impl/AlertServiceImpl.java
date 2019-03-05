package com.latticeengines.monitor.alerts.service.impl;

import java.util.Arrays;

import javax.inject.Inject;

import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.monitor.exposed.alerts.service.AlertService;
import com.latticeengines.monitor.exposed.alerts.service.PagerDutyService;

@Component("alertService")
@Scope("prototype")
public class AlertServiceImpl implements AlertService {

    private static final Logger log = LoggerFactory.getLogger(AlertServiceImpl.class);
    private static final Marker fatal = MarkerFactory.getMarker("FATAL");

    @Value("${monitor.alert.service.enabled:false}")
    private boolean alertServiceEnabled;

    // @Autowired
    // private PagerDutyService pagerDutyEmailService;

    @Inject
    private PagerDutyService pagerDutyService;

    @Override
    public String triggerCriticalEvent(String description, String clientUrl, String dedupKey,
            BasicNameValuePair... details) {
        return triggerCriticalEvent(description, clientUrl, dedupKey, Arrays.asList(details));
    }

    @Override
    public String triggerCriticalEvent(String description, String clientUrl, String dedupKey,
            Iterable<? extends BasicNameValuePair> details) {
//        if (!this.alertServiceEnabled) {
//            return "disabled";
//        }

        try {
            // return pagerDutyEmailService.triggerEvent(description, clientUrl, dedupKey, details);
            return pagerDutyService.triggerEvent(description, clientUrl, dedupKey, details);
        } catch (Exception e) {
            // Intentionally log and consume error
            log.error(fatal, String.format("Problem sending event to PagerDuty. description:%s clientUrl:%s dedupKey:%s",
                    description, clientUrl, dedupKey), e);
            return "fail";
        }
    }

    public void enableTestMode() {
        this.alertServiceEnabled = true;
        // ((PagerDutyEmailServiceImpl) this.pagerDutyEmailService).useTestService();
        ((PagerDutyServiceImpl) this.pagerDutyService).useTestServiceApiKey();
    }
}
