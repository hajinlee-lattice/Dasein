package com.latticeengines.monitor.exposed.alerts.service;

import org.apache.http.message.BasicNameValuePair;

public interface AlertService {

    String triggerCriticalEvent(String description, String clientUrl, String dedupKey,
            Iterable<? extends BasicNameValuePair> details);

    String triggerCriticalEvent(String description, String clientUrl, String dedupKey, BasicNameValuePair... details);

}
