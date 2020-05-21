package com.latticeengines.remote.service.tray.impl;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.remote.tray.TraySettings;
import com.latticeengines.proxy.exposed.RestApiClient;
import com.latticeengines.remote.exposed.service.tray.TrayService;

@Component("TrayService")
public class TrayServiceImpl implements TrayService {

    private static final Logger log = LoggerFactory.getLogger(TrayServiceImpl.class);

    @Value("${remote.tray.graphql.url}")
    private String trayGraphQLurl;

    @Inject
    private ApplicationContext applicationContext;

    private RestApiClient trayClient;

    @PostConstruct
    public void initialize() throws Exception {
        trayClient = RestApiClient.newExternalClient(applicationContext);
    }

    @Override
    public Object removeSolutionInstance(TraySettings settings) {
        Object returnObj = null;
        try {
            log.info(String.format("Trying to delete solution instance with settings %s",
                    JsonUtils.serialize(settings)));

            String query = String.format(
                    "{\"query\":\"mutation($solutionInstanceId: ID!) { removeSolutionInstance(input: {    solutionInstanceId: $solutionInstanceId\\n  }) {  clientMutationId\\n }}\",\"variables\":{\"solutionInstanceId\":\"%s\"}}",
                    settings.getSolutionInstanceId());
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", "application/json");
            headers.put("Authorization", String.format("bearer %s", settings.getUserToken()));

            returnObj = trayClient.postWithHeaders(trayGraphQLurl, query, headers, Object.class);
            log.info(String.format("Returned object is %s", returnObj));
        } catch (Exception e) {
            log.error("Failed to remove Tray solution instance", e);
            log.error(ExceptionUtils.getStackTrace(e));
        }
        return returnObj;
    }

}
