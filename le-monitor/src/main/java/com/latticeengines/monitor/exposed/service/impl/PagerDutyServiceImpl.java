package com.latticeengines.monitor.exposed.service.impl;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.monitor.exposed.service.PagerDutyService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.sun.tools.jdi.LinkedHashMap;

@Component("pagerDutyService")
public class PagerDutyServiceImpl implements PagerDutyService {

    private static final String MODELINGPLATFORM_SERVICEAPI_KEY = "62368b3c576e4f6180dba752216fd487";
    private static final String TEST_SERVICEAPI_KEY = "c6ca7f8f643c4db4a475bae9a504552d";

    private static List<BasicNameValuePair> headers = new ArrayList<BasicNameValuePair>();
    static {
        headers.add(new BasicNameValuePair("Authorization", "Token token=VjqbZdWQbwq2Fy7gniny"));
        headers.add(new BasicNameValuePair("Content-type", "application/json"));
    }

    private String serviceApiKey = MODELINGPLATFORM_SERVICEAPI_KEY;

    public void triggerEvent(String description, String clientUrl, BasicNameValuePair... details) {
        triggerEvent(description, clientUrl, Arrays.asList(details));
    }

    public void triggerEvent(String description, String clientUrl, Iterable<? extends BasicNameValuePair> details) {
        
        JSONObject obj=new JSONObject();
        LinkedHashMap detailsMap = new LinkedHashMap();
        obj.put("service_key", serviceApiKey);
        obj.put("event_type", "trigger");
        obj.put("description", description);
        obj.put("client", "Modeling Platform");
        obj.put("client_url", clientUrl);
        for (Iterator<? extends BasicNameValuePair> iterator = details.iterator(); iterator.hasNext();) {
            BasicNameValuePair detail = iterator.next();
            detailsMap.put(detail.getName(), detail.getValue());
        }
        obj.put("details", detailsMap);

        // response should look like this -
        // {"status":"success","message":"Event processed","incident_key":”acdcfa307f3e47d1b42b37edcbf22ae7"} 
		String response = "";
		JSONObject resultObj = null;
		try {
	        response = HttpClientWithOptionalRetryUtils.sendPostRequest(
	                "https://events.pagerduty.com/generic/2010-04-15/create_event.json", true, headers, obj.toString());
	        JSONParser parser = new JSONParser();
			resultObj = (JSONObject) parser.parse(response);
        } catch (ClientProtocolException e1){
        	throw new LedpException(LedpCode.LEDP_18003, e1);
        } catch (IOException e2) {
        	throw new LedpException(LedpCode.LEDP_18004, e2);
		} catch (ParseException e3) {
			throw new LedpException(LedpCode.LEDP_18005, e3);
		}
		
        if (!resultObj.get("status").equals("success")) {
        	throw new LedpException(LedpCode.LEDP_18001);
        }
    }

    @VisibleForTesting
    void useTestServiceApiKey() {
        serviceApiKey = TEST_SERVICEAPI_KEY;
    }

    @VisibleForTesting
    String getEvents() throws ClientProtocolException, IOException {
        String response = HttpClientWithOptionalRetryUtils.sendGetRequest("https://lattice-engines.pagerduty.com/api/v1/alerts", true, headers,
                new BasicNameValuePair("since", "2014-09-15T15:28-05"), new BasicNameValuePair("until",
                        "2014-10-15T15:30-05"));

        return response;
    }
}
