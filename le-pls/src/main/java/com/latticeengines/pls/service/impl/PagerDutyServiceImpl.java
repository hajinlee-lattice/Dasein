package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.JSONObject;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.pls.service.PagerDutyService;

@Component("pagerDutyService")
public class PagerDutyServiceImpl implements PagerDutyService {

    private static final String PLS_SERVICEAPI_KEY = "d3eb9c2d98b34f12a5a4915525a2e3ed";
    private static final String TEST_SERVICEAPI_KEY = "e0ac6d90e42442cf91a814b162a84796";

    private static List<BasicNameValuePair> headers = new ArrayList<>();
    static {
        headers.add(new BasicNameValuePair("Authorization", "Token token=UxFR6wR3EjVM1yRK6yZR"));
        headers.add(new BasicNameValuePair("Content-type", "application/json"));
    }
    private String serviceApiKey = PLS_SERVICEAPI_KEY;

    public String triggerEvent(String description, BasicNameValuePair... details)
            throws ClientProtocolException, IOException {
        return triggerEvent(description, Arrays.asList(details));
    }

    @SuppressWarnings("unchecked")
    public String triggerEvent(String description, Iterable<? extends BasicNameValuePair> details)
            throws ClientProtocolException, IOException {
        LinkedHashMap<String, String> mainMap = new LinkedHashMap<>();
        LinkedHashMap<String, String> detailsMap = new LinkedHashMap<>();
        mainMap.put("service_key", serviceApiKey);
        mainMap.put("event_type", "trigger");
        mainMap.put("description", description);
        mainMap.put("client", "PLS Multi-tenant");
        JSONObject obj=new JSONObject(mainMap);

        for (Iterator<? extends BasicNameValuePair> iterator = details.iterator(); iterator.hasNext();) {
            BasicNameValuePair detail = iterator.next();
            detailsMap.put(detail.getName(), detail.getValue());
        }
        obj.put("details", detailsMap);
        // response should look like this -
        // {"status":"success","message":"Event processed","incident_key":”acdcfa307f3e47d1b42b37edcbf22ae7"}
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(
                "https://events.pagerduty.com/generic/2010-04-15/create_event.json", false, headers, obj.toString());
        return response;
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

