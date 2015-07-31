package com.latticeengines.release.jira.service.impl;

import java.util.Arrays;
import java.util.HashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.release.exposed.domain.JiraParameters;
import com.latticeengines.release.jira.service.ChangeManagementJiraService;
import com.latticeengines.release.resttemplate.util.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.release.resttemplate.util.RestTemplateUtil;

@Service("changeManagmentJiraService")
public class ChangeManagementJiraServiceImpl implements ChangeManagementJiraService{

    @Autowired
    private RestTemplate restTemplate;
    
    @Value("${release.jira.url}")
    private String url;

    @Value("${release.jira.user.credential}")
    private String creds;

    @Override
    public ResponseEntity<String> createChangeManagementTicket(JiraParameters jiraParameters) {
        AuthorizationHeaderHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(RestTemplateUtil.encodeToken(creds));
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{interceptor}));
        return restTemplate.postForEntity(url, jiraParameters, String.class, new HashMap<>());
    }

}
