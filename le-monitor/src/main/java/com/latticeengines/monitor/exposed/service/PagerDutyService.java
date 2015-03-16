package com.latticeengines.monitor.exposed.service;

import org.apache.http.message.BasicNameValuePair;

public interface PagerDutyService {

    void triggerEvent(String description, String clientUrl, Iterable<? extends BasicNameValuePair> details);

    void triggerEvent(String description, String clientUrl, BasicNameValuePair... details);

}