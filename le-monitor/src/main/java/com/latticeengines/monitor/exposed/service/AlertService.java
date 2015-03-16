package com.latticeengines.monitor.exposed.service;

import org.apache.http.message.BasicNameValuePair;

public interface AlertService {

    void triggerCriticalEvent(String description, String clientUrl, BasicNameValuePair... details);
    
    void triggerCriticalEvent(String description, String clientUrl, Iterable<? extends BasicNameValuePair> details);

}
