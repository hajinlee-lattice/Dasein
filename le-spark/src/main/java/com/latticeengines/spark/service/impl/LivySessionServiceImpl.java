package com.latticeengines.spark.service.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.spark.exposed.service.LivySessionService;

@Service("livySessionService")
public class LivySessionServiceImpl implements LivySessionService {

    private static final Logger log = LoggerFactory.getLogger(LivySessionServiceImpl.class);

    private static final String URI_SESSIONS = "/sessions";

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
    private ObjectMapper om = new ObjectMapper();

    @Override
    public LivySession startSession(@NotNull String host, @NotNull String name, //
            Map<String, Object> livyConf, Map<String, String> sparkConf) {
        Map<String, Object> payLoad = new HashMap<>();
        payLoad.put("queue", "default");
        if (StringUtils.isNotBlank(name)) {
            payLoad.put("name", NamingUtils.uuid(name));
        }
        Map<String, String> conf = new HashMap<>();
        conf.put("livy.rsc.launcher.port.range", "10000~10999");
        if (MapUtils.isNotEmpty(sparkConf)) {
            conf.putAll(sparkConf);
        }
        Set<String> pkgs = new HashSet<>(getSparkPackages());
        if (sparkConf.containsKey("spark.jars.packages")) {
            pkgs.addAll(Arrays.asList(sparkConf.get("spark.jars.packages").split(",")));
        }
        conf.put("spark.jars.packages", StringUtils.join(pkgs, ","));
        log.info("conf=" + JsonUtils.serialize(conf));
        payLoad.put("conf", conf);
        if (MapUtils.isNotEmpty(livyConf)) {
            payLoad.putAll(livyConf);
            log.info("livyConf=" + JsonUtils.serialize(livyConf));
        }
        String url = host + URI_SESSIONS;
        String resp;
        try {
            resp = restTemplate.postForObject(url, payLoad, String.class);
        } catch (HttpClientErrorException e) {
            log.error("HttpClientErrorException: " + e.getResponseBodyAsString());
            throw e;
        }
        log.info("Starting new livy session on " + host + ": " + resp);
        int sessionId = parseSessionId(resp);
        LivySession session = new LivySession(host, sessionId);
        session = waitForSessionState(session, LivySession.STATE_IDLE);
        log.info("Livy session started: " + JsonUtils.serialize(session));
        return session;
    }

    @Override
    public LivySession getSession(LivySession session) {
        String info = getSessionInfo(session);
        return parseSessionInfo(session, info);
    }

    @Override
    public void stopSession(LivySession session) {
        Integer sessionId = session.getSessionId();
        if (sessionId != null && sessionExists(session)) {
            String url = session.getSessionUrl();
            restTemplate.delete(url);
            log.info("Stopped livy session " + session.getAppId() + " : " + session.getSessionUrl());
        }
    }

    private String getSessionInfo(LivySession session) {
        Integer sessionId = session.getSessionId();
        String info = "";
        if (sessionId != null) {
            String url = session.getSessionUrl();
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            info = retry.execute(ctx -> {
                try {
                    return restTemplate.getForObject(url, String.class);
                } catch (HttpClientErrorException.NotFound e) {
                    return "";
                }
            });
        }
        return info;
    }

    private boolean sessionExists(LivySession session) {
        String info = getSessionInfo(session);
        return StringUtils.isNotBlank(info);
    }

    private LivySession parseSessionInfo(LivySession session, String response) {
        if (StringUtils.isNotBlank(response)) {
            JsonNode json;
            try {
                json = om.readTree(response);
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse livy response: " + response, e);
            }
            String appId = json.get("appId").asText();
            String state = json.get("state").asText();
            String driverLogUrl = json.get("appInfo").get("driverLogUrl").asText();
            String sparkUiUrl = json.get("appInfo").get("sparkUiUrl").asText();
            session.setState(state);
            session.setAppId(appId);
            session.setDriverLogUrl(driverLogUrl);
            session.setSparkUiUrl(sparkUiUrl);
            return session;
        } else {
            return null;
        }
    }

    private int parseSessionId(String response) {
        try {
            JsonNode jsonNode = om.readTree(response);
            return jsonNode.get("id").asInt();
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse session id from livy response: " + response,
                    e);
        }
    }

    private LivySession waitForSessionState(LivySession session, String state) {
        LivySession current = getSession(session);
        while (!LivySession.TERMINAL_STATES.contains(current.getState())) {
            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            current = getSession(session);
            log.debug("Current session state: " + current.getState());
        }
        if (state.equals(current.getState())) {
            return current;
        } else {
            throw new RuntimeException(
                    "Session state ends up to be " + current.getState() + " instead of " + state);
        }
    }

    private List<String> getSparkPackages() {
        return Arrays.asList( //
                "org.apache.livy:livy-scala-api_2.11:0.6.0-incubating", //
                "com.fasterxml.jackson.module:jackson-module-scala_2.11:2.9.6", //
                "org.apache.spark:spark-avro_2.11:2.4.2" //
        );
    }

}
