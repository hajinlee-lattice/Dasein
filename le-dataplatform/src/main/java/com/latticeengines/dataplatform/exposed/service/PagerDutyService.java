package com.latticeengines.dataplatform.exposed.service;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;

public interface PagerDutyService {

    String triggerEvent(String description, String clientUrl, BasicNameValuePair... details) throws ClientProtocolException,
            IOException;

}
