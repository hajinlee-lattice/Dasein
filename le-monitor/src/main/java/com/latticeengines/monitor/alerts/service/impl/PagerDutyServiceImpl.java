package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HeaderRequestInterceptor;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.monitor.exposed.alerts.service.PagerDutyService;

@Component("pagerDutyService")
public class PagerDutyServiceImpl implements PagerDutyService {

    private static final Logger log = LoggerFactory.getLogger(PagerDutyServiceImpl.class);

    private static final String PLS_MODELINGPLATFORM_SCORING_SERVICEAPI_KEY = "62368b3c576e4f6180dba752216fd487";
    private static final String TEST_SERVICEAPI_KEY = "c6ca7f8f643c4db4a475bae9a504552d";
    private static final String TOKEN = "VjqbZdWQbwq2Fy7gniny";
    private static final String MODULE_NAME = "PLS, Modeling Platform, Scoring";

    protected String serviceApiKey;
    private static ObjectMapper om = new ObjectMapper();
    private static RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    @Value("${monitor.alert.service.enabled:false}")
    private boolean alertServiceEnabled;

    @Value("${monitor.alert.service.callapi.enabled:true}")
    private boolean alertServiceCallAPIEnabled;

    public PagerDutyServiceImpl() {
        this.serviceApiKey = PLS_MODELINGPLATFORM_SCORING_SERVICEAPI_KEY;

        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>();
        List<BasicNameValuePair> headers = this.getHeaders();
        for (BasicNameValuePair header : headers) {
            interceptors.add(new HeaderRequestInterceptor(header.getName(), header.getValue()));
        }
        this.restTemplate.setInterceptors(interceptors);
    }

    @Override
    public String triggerEvent(String description, String clientUrl, String dedupKey, BasicNameValuePair... details)
            throws IOException {
        return this.triggerEvent(description, clientUrl, dedupKey, Arrays.asList(details));
    }

    @Override
    public String triggerEvent(String description, String clientUrl, String dedupKey, Iterable<? extends BasicNameValuePair> details)
            throws IOException {
        // response should look like this -
        // {"status":"success","message":"Event processed","incident_key":”acdcfa307f3e47d1b42b37edcbf22ae7"}
        String payload = getRequestPayload(description, clientUrl, dedupKey, details);

        JsonNode filterJson = getFilterJsonNode();
        if(filterJson != null) {
            JsonNode contentJson = om.readTree(payload);
            if(!filterEvent(contentJson.get("description").toString(), filterJson.findValues("subject"))) {
                return "filterSubjectFail";
            }
            if(!filterEvent(contentJson.get("details").toString(), filterJson.findValues("body"))) {
                return "filterBodyFail";
            }
        } else {
            log.warn("No filter in zk.");
        }

        if(alertServiceEnabled && alertServiceCallAPIEnabled) {
            log.info("Trigger event by call API to PagerDuty: " + description + ". CallAPI");
            return restTemplate.postForObject("https://events.pagerduty.com/generic/2010-04-15/create_event.json", payload,
                    String.class);
        } else {
            log.info("Trigger event by call API to PagerDuty: " + description + ". NotCallAPI");
            return "notCallAPI";
        }
    }

    @VisibleForTesting
    public void useTestServiceApiKey() {
        this.serviceApiKey = TEST_SERVICEAPI_KEY;
    }

    @VisibleForTesting
    String getEvents() throws IOException {
        String response = HttpClientWithOptionalRetryUtils.sendGetRequest(
                "https://lattice-engines.pagerduty.com/api/v1/alerts", true, this.getHeaders(), new BasicNameValuePair(
                        "since", "2014-09-15T15:28-05"), new BasicNameValuePair("until", "2014-10-15T15:30-05"));
        return response;
    }

    @SuppressWarnings("unchecked")
    private String getRequestPayload(String description, String clientUrl, String incidentKey,
                                       Iterable<? extends BasicNameValuePair> details) {
        ObjectNode payload = om.createObjectNode();

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
        payload.put("details", om.valueToTree(detailsMap));

        return JsonUtils.serialize(payload);
    }

    private List<BasicNameValuePair> getHeaders() {
        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair("Authorization", "Token token=" + TOKEN));
        headers.add(new BasicNameValuePair("Content-type", "application/json"));

        return headers;
    }

    private JsonNode getFilterJsonNode() {
        try {
            Camille c = CamilleEnvironment.getCamille();
            String content = c.get(PathBuilder.buildTriggerFilterPath(CamilleEnvironment.getPodId())).getData();

            return om.readTree(content);
        }catch (Exception e) {
            log.error("Get json node from zk failed.", e);
            return null;
        }
    }

    private boolean filterEvent(String content, List<JsonNode> nodes) {
        boolean result = true;
        for (JsonNode subjectNode : nodes.get(0)) {
            result = result &&  Pattern.matches(subjectNode.get("filter").asText(), content);
            result = subjectNode.get("condition").asBoolean() ? result : !result;

            if(!result) {
                return result;
            }
        }

        return result;
    }
}
