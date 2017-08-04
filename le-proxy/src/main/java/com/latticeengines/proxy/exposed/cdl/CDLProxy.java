package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("cdlProxy")
public class CDLProxy extends MicroserviceRestApiProxy {

    protected CDLProxy() {
        super("cdl");
    }

    @SuppressWarnings("unchecked")
    public ApplicationId consolidate(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/consolidate", shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("consolidate", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
    }

    @SuppressWarnings("unchecked")
    public ApplicationId profile(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/profile", shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("profile", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
    }

    @SuppressWarnings("unchecked")
    public boolean reset(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/reset", shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("kickoff reset", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return false;
        }
        String statusStr = responseDoc.getResult();
        return ("Success".equals(statusStr));
    }

    public String createDataFeedTask(String customerSpace, String source, String entity, String feedType,
                                     String metadata) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/tasks?source={source}" +
                        "&feedtype={feedtype}&entity={entity}",
                shortenCustomerSpace(customerSpace), source, feedType, entity);
        String taskIdStr = post("createDataFeedTask", url, metadata, String.class);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json;
        try {
            json = mapper.readValue(taskIdStr, JsonNode.class);
        } catch (IOException e) {
            return null;
        }
        return JsonUtils.getOrDefault(json.get("task_id"), String.class, "");
    }

    public ApplicationId submitImportJob(String customerSpace, String taskIdentifier, String importConfig) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/tasks/import" +
                "/{taskIdentifier}", customerSpace, taskIdentifier);
        String appIdStr = post("submitImportJob", url, importConfig, String.class);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json;
        try {
            json = mapper.readValue(appIdStr, JsonNode.class);
        } catch (IOException e) {
            return null;
        }
        String appId = JsonUtils.getOrDefault(json.get("application_id"), String.class, "");
        return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appId);
    }

}
