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
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.monitor.exposed.alerts.service.PagerDutyService;

@Component("pagerDutyService")
public class PagerDutyServiceImpl implements PagerDutyService {

    private static final String PLS_MODELINGPLATFORM_SCORING_SERVICEAPI_KEY = "62368b3c576e4f6180dba752216fd487";
    private static final String TEST_SERVICEAPI_KEY = "c6ca7f8f643c4db4a475bae9a504552d";
    private static final String TOKEN = "VjqbZdWQbwq2Fy7gniny";
    private static final String MODULE_NAME = "PLS, Modeling Platform, Scoring";

    protected String serviceApiKey;

    public PagerDutyServiceImpl() {
        this.serviceApiKey = PLS_MODELINGPLATFORM_SCORING_SERVICEAPI_KEY;
    }

    @Override
    public String triggerEvent(String description, String clientUrl, String dedupKey, BasicNameValuePair... details)
            throws ClientProtocolException, IOException {

        return this.triggerEvent(description, clientUrl, dedupKey, Arrays.asList(details));
    }

    @Override
    public String triggerEvent(String description, String clientUrl, String dedupKey, Iterable<? extends BasicNameValuePair> details)
            throws ClientProtocolException, IOException {
        // response should look like this -
        // {"status":"success","message":"Event processed","incident_key":”acdcfa307f3e47d1b42b37edcbf22ae7"}
        return HttpClientWithOptionalRetryUtils.sendPostRequest(
                "https://events.pagerduty.com/generic/2010-04-15/create_event.json", true, this.getHeaders(), this
                        .getRequestPayload(description, clientUrl, dedupKey, details).toString());
    }

    @VisibleForTesting
    public void useTestServiceApiKey() {
        this.serviceApiKey = TEST_SERVICEAPI_KEY;
    }

    @VisibleForTesting
    String getEvents() throws ClientProtocolException, IOException {
        String response = HttpClientWithOptionalRetryUtils.sendGetRequest(
                "https://lattice-engines.pagerduty.com/api/v1/alerts", true, this.getHeaders(), new BasicNameValuePair(
                        "since", "2014-09-15T15:28-05"), new BasicNameValuePair("until", "2014-10-15T15:30-05"));
        return response;
    }

    @SuppressWarnings("unchecked")
    private JSONObject getRequestPayload(String description, String clientUrl, String incidentKey,
            Iterable<? extends BasicNameValuePair> details) {
        JSONObject payload = new JSONObject();

        LinkedHashMap<String, String> detailsMap = new LinkedHashMap<>();
        payload.put("service_key", this.serviceApiKey);
        payload.put("event_type", "trigger");
        payload.put("description", Strings.nullToEmpty(description));
        if (!StringUtils.isEmpty(incidentKey)) {
            payload.put("incident_key", incidentKey);
        }
        payload.put("client", MODULE_NAME);
        if (!StringUtils.isEmpty(clientUrl)) {
            payload.put("client_url", clientUrl);
        }

        for (Iterator<? extends BasicNameValuePair> iterator = details.iterator(); iterator.hasNext();) {
            BasicNameValuePair detail = iterator.next();
            detailsMap.put(detail.getName(), detail.getValue());
        }
        payload.put("details", detailsMap);

        return payload;
    }

    private List<BasicNameValuePair> getHeaders() {
        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair("Authorization", "Token token=" + TOKEN));
        headers.add(new BasicNameValuePair("Content-type", "application/json"));

        return headers;
    }
}
