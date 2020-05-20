package com.latticeengines.monitor.exposed.service.impl;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.monitor.TraySettings;
import com.latticeengines.monitor.exposed.service.TrayService;

@Component("TrayService")
public class TrayServiceImpl implements TrayService {

    private static final Logger log = LoggerFactory.getLogger(TrayServiceImpl.class);

    @Value("${monintor.tray.graphql.url}")
    private String trayGraphQLurl;

    private RestTemplate trayRestTemplate = HttpClientUtils.newSSLEnforcedRestTemplate();

    @Override
    public Object removeSolutionInstance(TraySettings settings) {
        Object returnObj = null;
        try {
            log.info(String.format("Trying to delete solution instance with settings %s",
                    JsonUtils.serialize(settings)));

            String query = String.format(
                    "{\"query\":\"mutation($solutionInstanceId: ID!) { removeSolutionInstance(input: {    solutionInstanceId: $solutionInstanceId\\n  }) {  clientMutationId\\n }}\",\"variables\":{\"solutionInstanceId\":\"%s\"}}",
                    settings.getSolutionInstanceId());
            HttpHeaders headers = new HttpHeaders();
            headers.set("Content-Type", "application/json");
            headers.set("Authorization", String.format("bearer %s", settings.getUserToken()));
            HttpEntity<String> request = new HttpEntity<>(query, headers);

            returnObj = trayRestTemplate.postForObject(trayGraphQLurl, request, Object.class);
            log.info(String.format("Returned object is %s", returnObj));
        } catch (Exception e) {
            log.error("Failed to remove Tray solution instance", e);
            log.error(ExceptionUtils.getStackTrace(e));
        }
        return returnObj;
    }

}
