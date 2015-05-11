package com.latticeengines.monitor.exposed.alerts.service;

import org.apache.http.message.BasicNameValuePair;

public interface AlertService {

    String triggerCriticalEvent(String description, String clientUrl, Iterable<? extends BasicNameValuePair> details);

    String triggerCriticalEvent(String description, String clientUrl, BasicNameValuePair... details);

}
