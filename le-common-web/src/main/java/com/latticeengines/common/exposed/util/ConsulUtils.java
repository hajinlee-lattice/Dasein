package com.latticeengines.common.exposed.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;

public class ConsulUtils {

    public static String getValueFromConsul(String consulUrl, String key) {
        String kvUrl = consulUrl + "/v1/kv/" + key;
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        RetryTemplate retry = RetryUtils.getRetryTemplate(5, null, //
                Collections.singleton(HttpClientErrorException.NotFound.class));
        try {
            JsonNode json = retry.execute(context -> restTemplate.getForObject(kvUrl, JsonNode.class));
            if (json != null && json.size() >= 1) {
                String data = json.get(0).get("Value").asText();
                return new String(Base64Utils.decodeBase64(data));
            }
        } catch (HttpClientErrorException.NotFound e) {
            throw new RuntimeException("key doesn't exists in the consul KV store: " + e);
        }
        return null;
    }

    public static boolean isKeyExists(String consulUrl, String key) {
        String kvUrl = consulUrl + "/v1/kv/" + key;
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        try {
            JsonNode json = restTemplate.getForObject(kvUrl, JsonNode.class);

            return (json != null && json.size() >= 1) ? true : false;
        } catch (RestClientException e) {
            return false;
        }
    }
}
