package com.latticeengines.remote.service.tray.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.remote.tray.TraySettings;
import com.latticeengines.proxy.exposed.RestApiClient;
import com.latticeengines.remote.exposed.service.tray.TrayService;

@Component("TrayService")
public class TrayServiceImpl implements TrayService {

    private static final Logger log = LoggerFactory.getLogger(TrayServiceImpl.class);

    @Value("${remote.tray.graphql.token}")
    private String trayGraphQLToken;

    @Value("${remote.tray.graphql.url}")
    private String trayGraphQLurl;

    @Inject
    private ApplicationContext applicationContext;

    private volatile RestApiClient trayClient;

    @SuppressWarnings("unchecked")
    @Override
    public Object removeSolutionInstance(TraySettings settings) {
        log.info(String.format("Trying to delete solution instance with settings %s", JsonUtils.serialize(settings)));

        String query = String.format(
                "{" +
                    "\"query\":\"mutation($solutionInstanceId: ID!) {" +
                        "removeSolutionInstance(input: {solutionInstanceId: $solutionInstanceId}) {" +
                            "clientMutationId" +
                        "}" +
                    "}\"," +
                    "\"variables\":{\"solutionInstanceId\":\"%s\"}" +
                "}",
                settings.getSolutionInstanceId());

        Object returnObj = callTrayGraphApi(query, settings.getUserToken());
        return returnObj;
    }

    @Override
    public Object removeAuthentication(TraySettings settings) {
        return removeAuthenticationById(settings.getAuthenticationId(), settings.getUserToken());
    }

    @Override
    public Object removeAuthenticationById(String authId, String userToken) {
        log.info("Removing Tray Auth with ID: " + authId);

        String query = String.format(
                "{" +
                    "\"query\": \"mutation($authenticationId: ID!) {" +
                        "removeAuthentication(input: {authenticationId: $authenticationId}) {" +
                            "clientMutationId" +
                        "}" +
                    "}\"," +
                    "\"variables\": {\"authenticationId\":\"%s\"}" +
                "}",
                authId);

        Object returnObj = callTrayGraphApi(query, userToken);
        return returnObj;
    }

    @Override
    public String getTrayUserToken(String trayUserId) {
        log.info("Getting Tray User Token for Tray User " + trayUserId);

        String query = String.format(
                "{" +
                    "\"query\": \"mutation($userId: ID!) {" +
                        "authorize(input: { userId: $userId }) {" +
                            "accessToken" +
                        "}" +
                    "}\"," +
                    "\"variables\": {\"userId\":\"%s\"}" +
                "}",
                trayUserId);

        Map<String, Object> returnObj = (Map<String, Object>) callTrayGraphApi(query, trayGraphQLToken);

        String userToken = "";
        if (returnObj != null) {
            Map <String, Object> data = (Map<String, Object>) returnObj.get("data");
            Map <String, String> authorize = (Map<String, String>) data.get("authorize");
            userToken = authorize.get("accessToken");
        }
        return userToken;
    }

    private RestApiClient getTrayClient() {
        if (trayClient == null) {
            constructClient();
        }
        return trayClient;
    }

    private synchronized void constructClient() {
        if (trayClient == null) {
            trayClient = RestApiClient.newExternalClient(applicationContext);
        }
    }

    private Object callTrayGraphApi(String query, String token) {
        Object returnObj = null;

        Map<String, String> headers = new HashMap<>();
        headers.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        headers.put(HttpHeaders.AUTHORIZATION, String.format("bearer %s", token));

        returnObj = getTrayClient().postWithHeaders(trayGraphQLurl, query, headers, Object.class);
        log.info(String.format("Returned object is %s", returnObj));

        if (((Map<String, List<Object>>) returnObj).get("errors") != null) {
            String errorMessage = ((Map<String, String>) ((Map<String, List<Object>>) returnObj).get("errors").get(0))
                    .get("message");
            throw new LedpException(LedpCode.LEDP_31000, new String[] { errorMessage });
        }
        return returnObj;
    }

}
