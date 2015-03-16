package com.latticeengines.monitor.exposed.service;

import java.io.IOException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.parser.ParseException;

public interface JiraService {
	
    void triggerEvent(String description, String clientUrl, Iterable<? extends BasicNameValuePair> details)
            throws ClientProtocolException, IOException, ParseException;
	
    void triggerEvent(String description, String clientUrl, BasicNameValuePair... details) throws ClientProtocolException,
    IOException, ParseException;
    
}
