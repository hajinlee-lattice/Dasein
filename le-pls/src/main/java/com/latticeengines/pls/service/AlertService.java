package com.latticeengines.pls.service;

import org.apache.http.message.BasicNameValuePair;

public interface AlertService {
	
	String triggerCriticalEvent(String description, Iterable<? extends BasicNameValuePair> details);

    String triggerCriticalEvent(String description, BasicNameValuePair... details);

}
