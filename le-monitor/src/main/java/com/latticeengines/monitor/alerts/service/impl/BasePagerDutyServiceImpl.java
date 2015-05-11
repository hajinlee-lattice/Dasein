package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.JSONObject;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.monitor.exposed.alerts.service.PagerDutyService;

public abstract class BasePagerDutyServiceImpl implements PagerDutyService {

    protected String serviceApiKey;

    abstract protected String getToken();

    abstract protected String getModuleName();

    abstract protected String getTestServiceApiKey();

    public String triggerEvent(String description, String clientUrl, BasicNameValuePair... details)
            throws ClientProtocolException, IOException {
        return triggerEvent(description, clientUrl, Arrays.asList(details));
    }

    @SuppressWarnings("unchecked")
    public String triggerEvent(String description, String clientUrl, Iterable<? extends BasicNameValuePair> details)
            throws ClientProtocolException, IOException {
        LinkedHashMap<String, String> mainMap = new LinkedHashMap<>();
        LinkedHashMap<String, String> detailsMap = new LinkedHashMap<>();
        mainMap.put("service_key", serviceApiKey);
        mainMap.put("event_type", "trigger");
        mainMap.put("description", description);
        mainMap.put("client", getModuleName());
        if (!StringUtils.isEmpty(clientUrl)) {
            mainMap.put("client_url", clientUrl);
        }
        JSONObject obj = new JSONObject(mainMap);

        for (Iterator<? extends BasicNameValuePair> iterator = details.iterator(); iterator.hasNext();) {
            BasicNameValuePair detail = iterator.next();
            detailsMap.put(detail.getName(), detail.getValue());
        }
        obj.put("details", detailsMap);

        // response should look like this -
        // {"status":"success","message":"Event processed","incident_key":”acdcfa307f3e47d1b42b37edcbf22ae7"}
        String response = HttpClientWithOptionalRetryUtils
                .sendPostRequest("https://events.pagerduty.com/generic/2010-04-15/create_event.json", true,
                        getHeaders(), obj.toString());

        return response;
    }

    private List<BasicNameValuePair> getHeaders() {
        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair("Authorization", "Token token=" + getToken()));
        headers.add(new BasicNameValuePair("Content-type", "application/json"));

        return headers;
    }

    @VisibleForTesting
    public void useTestServiceApiKey() {
        serviceApiKey = getTestServiceApiKey();
    }

    @VisibleForTesting
    String getEvents() throws ClientProtocolException, IOException {
        String response = HttpClientWithOptionalRetryUtils.sendGetRequest(
                "https://lattice-engines.pagerduty.com/api/v1/alerts", true, getHeaders(), new BasicNameValuePair(
                        "since", "2014-09-15T15:28-05"), new BasicNameValuePair("until", "2014-10-15T15:30-05"));

        return response;
    }
}
