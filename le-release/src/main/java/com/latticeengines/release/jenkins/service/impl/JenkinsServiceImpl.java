package com.latticeengines.release.jenkins.service.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.release.exposed.domain.JenkinsParameters;
import com.latticeengines.release.jenkins.service.JenkinsService;
import com.latticeengines.release.resttemplate.util.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.release.resttemplate.util.RestTemplateUtil;


@Service("jenkinsService")
public class JenkinsServiceImpl implements JenkinsService{

    @Value("${release.jenkins.deploymenttest.url}")
    private String deploymentTestUrl;

    @Value("${release.jenkins.user.credential}")
    private String creds;

    @Autowired
    private RestTemplate restTemplate;
    
    private static final String PARAMETERS = "parameters";

    @Override
    public ResponseEntity<String> triggerJenkinsJobWithOutParameters(String url) {
        AuthorizationHeaderHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(RestTemplateUtil.encodeToken(creds));
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{interceptor}));
        return restTemplate.postForEntity(deploymentTestUrl + "/buildWithParameters", "", String.class, new HashMap<>());
    }

    @Override
    public ResponseEntity<String> triggerJenkinsJobWithParameters(String url, JenkinsParameters jenkinsParameters) {
        AuthorizationHeaderHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(RestTemplateUtil.encodeToken(creds));
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{interceptor}));
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put(PARAMETERS, JsonUtils.serialize(jenkinsParameters));
        return restTemplate.postForEntity(deploymentTestUrl + "/build?json={" + PARAMETERS + "}", "", String.class, uriVariables);
    }

    @Override
    public JsonNode getLastBuildStatus(String url) throws JsonProcessingException, IOException {
        AuthorizationHeaderHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(RestTemplateUtil.encodeToken(creds));
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{interceptor}));
        String response = restTemplate.getForObject(deploymentTestUrl + "/lastBuild/api/json", String.class, new HashMap<>());
        return new ObjectMapper().readTree(response);
    }
}
