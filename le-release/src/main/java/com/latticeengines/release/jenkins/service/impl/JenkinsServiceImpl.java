package com.latticeengines.release.jenkins.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.w3c.dom.Document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.release.exposed.domain.JenkinsBuildStatus;
import com.latticeengines.release.exposed.domain.JenkinsParameters;
import com.latticeengines.release.jenkins.service.JenkinsService;
import com.latticeengines.release.jenkins.xml.helper.JenkinsXMLHelper;
import com.latticeengines.release.resttemplate.util.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.release.resttemplate.util.RestTemplateUtil;

@Service("jenkinsService")
public class JenkinsServiceImpl implements JenkinsService {

    @Value("${release.jenkins.user.credential}")
    private String creds;

    @Autowired
    private RestTemplate restTemplate;

    private static final String PARAMETERS = "parameters";

    @Override
    public ResponseEntity<String> triggerJenkinsJobWithOutParameters(String url) {
        AuthorizationHeaderHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(
                RestTemplateUtil.encodeToken(creds));
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { interceptor }));
        return restTemplate.postForEntity(url + "/buildWithParameters", "", String.class);
    }

    @Override
    public ResponseEntity<String> triggerJenkinsJobWithParameters(String url, JenkinsParameters jenkinsParameters) {
        AuthorizationHeaderHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(
                RestTemplateUtil.encodeToken(creds));
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { interceptor }));
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put(PARAMETERS, JsonUtils.serialize(jenkinsParameters));
        return restTemplate.postForEntity(url + "/build?json={" + PARAMETERS + "}", "", String.class, uriVariables);
    }

    @Override
    public JenkinsBuildStatus getLastBuildStatus(String url) {
        AuthorizationHeaderHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(
                RestTemplateUtil.encodeToken(creds));
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { interceptor }));
        String response = restTemplate.getForObject(url + "/lastBuild/api/json", String.class);
        try {
            JsonNode node = new ObjectMapper().readTree(response);
            //System.out.println(node.get("actions").path(1).get("parameters").path(1).get("value"));
            return new JenkinsBuildStatus(node.get("building").asBoolean(), node.get("result").asText(), node.get(
                    "number").asLong());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResponseEntity<String> updateSVNBranchName(String url, String version) {
        try {
            String configuration = getConfiguration(url);
            Document document = JenkinsXMLHelper.updateVersionInXMLDocument(version, configuration);
            String newConfiguration = JenkinsXMLHelper.convertXMLDocumentToString(document);
            return restTemplate.postForEntity(url + "/config.xml", newConfiguration, String.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getConfiguration(String url) {
        AuthorizationHeaderHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(
                RestTemplateUtil.encodeToken(creds));
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { interceptor }));
        return restTemplate.getForObject(url + "/config.xml", String.class);
    }
}
