package com.latticeengines.spark.service.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.validation.constraints.NotNull;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.spark.exposed.service.LivySessionService;

import reactor.core.publisher.Mono;

@Service("livySessionService")
public class LivySessionServiceImpl implements LivySessionService {

    private static final Logger log = LoggerFactory.getLogger(LivySessionServiceImpl.class);

    private static final String URI_SESSIONS = "/sessions";

    private ConcurrentMap<String, WebClient> webClients = new ConcurrentHashMap<>();
    private ObjectMapper om = new ObjectMapper();

    @Override
    public LivySession startSession(@NotNull String host, @NotNull String name, //
            Map<String, Object> livyConf, Map<String, String> sparkConf) {
        Map<String, Object> payLoad = new HashMap<>();
        payLoad.put("queue", "default");
        if (StringUtils.isNotBlank(name)) {
            payLoad.put("name", name);
        }
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.jars.packages", getSparkPackages());
        if (MapUtils.isNotEmpty(sparkConf)) {
            conf.putAll(sparkConf);
        }
        payLoad.put("conf", conf);
        if (MapUtils.isNotEmpty(livyConf)) {
            payLoad.putAll(livyConf);
        }
        WebClient webClient = getWebClient(host);
        String resp = webClient.method(HttpMethod.POST).uri(URI_SESSIONS).syncBody(payLoad) //
                .retrieve().bodyToMono(String.class).block();
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
            WebClient webClient = getWebClient(session);
            String uri = URI_SESSIONS + "/" + sessionId;
            String resp = webClient.method(HttpMethod.DELETE).uri(uri).retrieve()
                    .bodyToMono(String.class).block();
            log.info("Stopped livy session: " + resp);
        }
    }

    private String getSessionInfo(LivySession session) {
        Integer sessionId = session.getSessionId();
        String info = "";
        if (sessionId != null) {
            WebClient webClient = getWebClient(session);
            String uri = URI_SESSIONS + "/" + sessionId;
            info = webClient.method(HttpMethod.GET).uri(uri) //
                    .retrieve().bodyToMono(String.class).onErrorResume(t -> {
                        Mono<String> resumed = Mono.error(t);
                        if (t instanceof WebClientResponseException) {
                            WebClientResponseException webException = (WebClientResponseException) t;
                            if (webException.getStatusCode().value() == 404) {
                                resumed = Mono.just("");
                            }
                        }
                        return resumed;
                    }).block();
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

    private WebClient getWebClient(LivySession session) {
        String host = session.getHost();
        return getWebClient(host);
    }

    private WebClient getWebClient(String host) {
        if (StringUtils.isBlank(host)) {
            throw new IllegalArgumentException("Must provide the livy host.");
        }
        if (!webClients.containsKey(host)) {
            log.info("Create a web client for livy host " + host);
            WebClient webClient = WebClient.builder() //
                    .baseUrl(host) //
                    .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE) //
                    .build();
            webClients.putIfAbsent(host, webClient);
        }
        return webClients.get(host);
    }

    private String getSparkPackages() {
        return StringUtils.join(Arrays.asList( //
                "org.apache.livy:livy-scala-api_2.11:0.5.0-incubating", //
                "com.fasterxml.jackson.module:jackson-module-scala_2.11:2.9.6", //
                "org.apache.spark:spark-avro_2.11:2.4.0" //
        ), ",");
    }

}
