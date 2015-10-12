package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.monitor.exposed.alerts.service.JiraService;

@Component("jiraService")
public class JiraServiceImpl implements JiraService {

    private final static Log log = LogFactory.getLog(JiraServiceImpl.class.getName());

    private static List<BasicNameValuePair> headers = new ArrayList<BasicNameValuePair>();
    static {
        headers.add(new BasicNameValuePair("Content-type", "application/json"));
        headers.add(new BasicNameValuePair("Authorization", "Basic amlyYVJFU1RBUEk6UEAkJHcwcmQx"));
    }

    public void triggerEvent(String description, String clientUrl, BasicNameValuePair... details) {
        triggerEvent(description, clientUrl, Arrays.asList(details));
    }

    @SuppressWarnings("unchecked")
	public void triggerEvent(String description, String clientUrl, Iterable<? extends BasicNameValuePair> details) {

        JSONObject obj = new JSONObject();
        Map<String, Object> fieldsMap = new HashMap<>();
        Map<String, Object> projectMap = new HashMap<>();
        Map<String, Object> issuetypeMap = new HashMap<>();
        Map<String, Object> priorityMap = new HashMap<>();
        Map<String, Object> componentsMap = new HashMap<>();
        Map<String, Object> customfield_11503Map = new HashMap<>();
        Map<String, Object> customfield_10249Map = new HashMap<>();
        Map<String, Object> customfield_11900Map = new HashMap<>();
        LinkedList<Map<String, Object>> customfield_10249List = new LinkedList<Map<String, Object>>();
        LinkedList<Map<String, Object>> componentsList = new LinkedList<Map<String, Object>>();
        projectMap.put("id", "12300");
        fieldsMap.put("project", projectMap);
        fieldsMap.put("summary", description);
        issuetypeMap.put("name", "Monitoring");
        fieldsMap.put("issuetype", issuetypeMap);
        priorityMap.put("name", "High");
        fieldsMap.put("priority", priorityMap);
        componentsMap.put("name", "PLS Modeling Platform");
        componentsList.add(componentsMap);
        fieldsMap.put("components", componentsList);
        customfield_11503Map.put("id", "14534");
        fieldsMap.put("customfield_11503", customfield_11503Map);
        customfield_10249Map.put("id", "10702");
        customfield_10249List.add(customfield_10249Map);
        fieldsMap.put("customfield_10249", customfield_10249List);
        customfield_11900Map.put("id", "13502");
        fieldsMap.put("customfield_11900", customfield_11900Map);
        StringBuilder descriptions = new StringBuilder();
        for (Iterator<? extends BasicNameValuePair> iterator = details.iterator(); iterator.hasNext();) {
            BasicNameValuePair detail = iterator.next();
            descriptions.append(detail.getName() + ": ");
            descriptions.append(detail.getValue() + "\n");
        }
        fieldsMap.put("description", descriptions.toString());
        obj.put("fields", fieldsMap);

        String response = "";
        JSONObject resultObj = null;
        try {
            // response should look like this -
            // {"id":"IDNumber","key":"TECHOPS-someNumber","self":"https://solutions.lattice-engines.com/rest/api/2/issue/IDNumber"}
            response = HttpClientWithOptionalRetryUtils.sendPostRequest(
                    "https://solutions.lattice-engines.com/rest/api/2/issue/", true, headers, obj.toString());
            JSONParser parser = new JSONParser();
            log.info("Here is the feedback from the Jira server:");
            log.info(response);
            resultObj = (JSONObject) parser.parse(response);
        } catch (ClientProtocolException e1) {
            log.error(e1.getMessage());
            e1.printStackTrace();
            throw new LedpException(LedpCode.LEDP_18003, e1);
        } catch (IOException e2) {
            log.error(e2.getMessage());
            e2.printStackTrace();
            throw new LedpException(LedpCode.LEDP_18004, e2);
        } catch (ParseException e3) {
            log.error(e3.getMessage());
            e3.printStackTrace();
            throw new LedpException(LedpCode.LEDP_18005, e3);
        }

        if (!resultObj.get("self").toString().contains(resultObj.get("id").toString())) {
            throw new LedpException(LedpCode.LEDP_18002);
        }
    }
}
