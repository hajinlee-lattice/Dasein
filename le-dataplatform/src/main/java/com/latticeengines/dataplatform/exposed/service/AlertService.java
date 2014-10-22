package com.latticeengines.dataplatform.exposed.service;

import org.apache.http.message.BasicNameValuePair;

public interface AlertService {

    String triggerCriticalEvent(String description, BasicNameValuePair... details);

}
