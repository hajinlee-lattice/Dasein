package com.latticeengines.pls.exposed.service;

import org.apache.http.message.BasicNameValuePair;

public interface AlertService {
	
	String triggerCriticalEvent(String description, Iterable<? extends BasicNameValuePair> details);

    String triggerCriticalEvent(String description, BasicNameValuePair... details);

}
