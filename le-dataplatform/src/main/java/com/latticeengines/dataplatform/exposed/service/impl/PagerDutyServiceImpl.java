package com.latticeengines.dataplatform.exposed.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HttpUtils;
import com.latticeengines.dataplatform.exposed.service.PagerDutyService;

@Component("pagerDutyService")
public class PagerDutyServiceImpl implements PagerDutyService {

    private static List<BasicNameValuePair> headers = new ArrayList<>();
    static {
        headers.add(new BasicNameValuePair("Authorization", "Token token=VjqbZdWQbwq2Fy7gniny"));
        headers.add(new BasicNameValuePair("Content-type", "application/json"));
    }

    public String triggerEvent(String description, BasicNameValuePair... details) throws ClientProtocolException,
            IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"service_key\": \"62368b3c576e4f6180dba752216fd487\",");
        sb.append("\"event_type\": \"trigger\",");
        sb.append("\"description\": \"" + description + "\",");
        sb.append("\"client\": \"Modeling Platform\",");
        sb.append("\"client_url\": \"http://production\",");
        sb.append("\"details\": {");
        for (int i = 0; i < details.length; i++) {
            BasicNameValuePair detail = details[0];
            sb.append("\"" + detail.getName() + "\":");
            sb.append("\"" + detail.getValue() + "\"");
            if ((i+1) < details.length) {
                sb.append(",");
            }
        }
        sb.append("}}");

        String response = HttpUtils.sendPostRequest(
                "https://events.pagerduty.com/generic/2010-04-15/create_event.json", headers, sb.toString());

        return response;
    }

    @VisibleForTesting
    String getEvents() throws ClientProtocolException, IOException {
        String response = HttpUtils.sendGetRequest("https://lattice-engines.pagerduty.com/api/v1/alerts", headers,
                new BasicNameValuePair("since", "2014-09-15T15:28-05"), new BasicNameValuePair("until",
                        "2014-10-15T15:30-05"));

        return response;
    }
}
