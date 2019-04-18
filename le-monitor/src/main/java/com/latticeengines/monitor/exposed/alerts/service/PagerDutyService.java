package com.latticeengines.monitor.exposed.alerts.service;

import java.io.IOException;

import org.apache.http.message.BasicNameValuePair;

public interface PagerDutyService {

    String triggerEvent(String description, String clientUrl, String dedupKey, Iterable<? extends BasicNameValuePair> details)
            throws IOException;

    String triggerEvent(String description, String clientUrl, String dedupKey, BasicNameValuePair... details)
            throws IOException;

}
