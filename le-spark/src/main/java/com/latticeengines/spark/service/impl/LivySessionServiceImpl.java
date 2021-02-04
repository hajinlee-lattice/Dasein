package com.latticeengines.spark.service.impl;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.http.conn.HttpHostConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.spark.exposed.service.LivySessionService;


@Service("livySessionService")
public class LivySessionServiceImpl implements LivySessionService {

    @Inject
    private LivyServerManager livyServerManager;

    @Inject
    private Configuration yarnConfiguration;

    @Value("${spark.jars.repositories}")
    private String sparkJarsRepo;

    @Value("${aws.default.access.key}")
    protected String awsKey;

    @Value("${aws.default.secret.key.encrypted}")
    protected String awsSecret;

    private static final Logger log = LoggerFactory.getLogger(LivySessionServiceImpl.class);

    private static final String URI_SESSIONS = "/sessions";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("~yyyy_MM_dd_HH_mm_ss_z");
    private static final long SESSION_CREATION_TIMEOUT = TimeUnit.MINUTES.toMillis(30);

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
    private ObjectMapper om = new ObjectMapper();

    @Override
    public LivySession startSession(@NotNull String name, //
            Map<String, Object> livyConf, Map<String, String> sparkConf) {
        Map<String, Object> payLoad = new HashMap<>();
        payLoad.put("queue", "default");
        if (StringUtils.isNotBlank(name)) {
            payLoad.put("name", name + DATE_FORMAT.format(new Date()));
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
        // Explicitly point to dnb artifactory if present in properties
        if (StringUtils.isNotEmpty(sparkJarsRepo)) {
            conf.put("spark.jars.repositories", sparkJarsRepo);
        }
        log.info("conf=" + JsonUtils.serialize(conf));
        // do not log these 2 configuration
        conf.put("spark.hadoop.fs.s3a.access.key", awsKey);
        conf.put("spark.hadoop.fs.s3a.secret.key", awsSecret);
        conf.put("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com");
        payLoad.put("conf", conf);
        if (MapUtils.isNotEmpty(livyConf)) {
            payLoad.putAll(livyConf);
            log.info("livyConf=" + JsonUtils.serialize(livyConf));
        }
        String host = livyServerManager.getLivyHost();
        String url = host + URI_SESSIONS;
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
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
        });
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
            try {
                restTemplate.delete(url);
            } catch (ResourceAccessException e) {
                if (e.getCause() instanceof SocketTimeoutException) {
                    log.warn("Encountered socket time out error when killing the livy session: {}", url, e.getCause());
                } else if (e.getCause() instanceof HttpHostConnectException) {
                    log.warn("Encountered connection exception when killing the livy session: {}", url, e.getCause());
                } else {
                    log.error("Encountered ResourceAccessException when killing the livy session: {}", url, e.getCause());
                }
            } catch (Exception e) {
                log.warn("Encountered exception when killing the livy session: {}", url, e);
            }
            log.info("Stopped livy session " + session.getAppId() + " : " + session.getSessionUrl());
        }
    }

    private String getSessionInfo(LivySession session) {
        Integer sessionId = session.getSessionId();
        String info = "";
        if (sessionId != null) {
            String url = session.getSessionUrl();
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            try {
                info = retry.execute(ctx -> {
                    try {
                        return restTemplate.getForObject(url, String.class);
                    } catch (HttpClientErrorException.NotFound e) {
                        return "";
                    }
                });
            } catch (ResourceAccessException e) {
                if (e.getCause() instanceof SocketTimeoutException) {
                    log.warn("Encountered socket time out error when fetching the livy session: {}", url, e.getCause());
                } else if (e.getCause() instanceof HttpHostConnectException) {
                    log.warn("Encountered connection exception when fetching the livy session: {}", url, e.getCause());
                } else {
                    log.warn("Encountered ResourceAccessException when fetching the livy session: {}", url, e.getCause());
                }
            } catch (Exception e) {
                log.warn("Encountered exception when fetching the livy session: {}", url, e);
            }
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
            JsonNode driverLogUrlNode = json.get("appInfo").get("driverLogUrl");
            JsonNode sparkUiUrlNode = json.get("appInfo").get("sparkUiUrl");
            String driverLogUrl = driverLogUrlNode.isNull() ? null : driverLogUrlNode.asText();
            String sparkUiUrl = sparkUiUrlNode.isNull() ? null : sparkUiUrlNode.asText();
            session.setState(state);
            session.setAppId(appId);
            session.setDriverLogUrl(driverLogUrl);
            session.setSparkUiUrl(sparkUiUrl);
            if (json.has("name")) {
                String appName = json.get("name").asText();
                session.setAppName(appName);
            }
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
        long start = System.currentTimeMillis();
        while (!LivySession.TERMINAL_STATES.contains(current.getState()) &&
                (System.currentTimeMillis() - start < SESSION_CREATION_TIMEOUT)) {
            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            current = getSession(session);
            log.debug("Current session state: " + current.getState());
        }
        if (state.equals(current.getState())) {
            String appName = current.getAppName();
            if (StringUtils.isNotBlank(appName)) {
                current.setAppId(getAppId(appName));
            }
            return current;
        } else {
            stopSession(current);
            throw new RuntimeException(
                    "Session state ends up to be " + current.getState() + " instead of " + state);
        }
    }

    private List<String> getSparkPackages() {
        return Arrays.asList( //
                "org.apache.livy:livy-scala-api_2.11:0.7.0-incubating", //
                "com.fasterxml.jackson.module:jackson-module-scala_2.11:2.10.1", //
                "org.apache.spark:spark-avro_2.11:2.4.7", //
                "commons-httpclient:commons-httpclient:3.1"
        );
    }

    private String getAppId(@NotNull final String appName) {
        String appId = null;
        try (YarnClient yarnClient = YarnClient.createYarnClient()) {
            yarnClient.init(yarnConfiguration);
            yarnClient.start();
            Set<String> appTypes = Sets.newHashSet("SPARK");
            EnumSet<YarnApplicationState> states = EnumSet.copyOf(Collections.singleton(YarnApplicationState.RUNNING));
            List<ApplicationReport> reports = yarnClient.getApplications(appTypes, states);
            yarnClient.stop();
            ApplicationReport report = reports.stream() //
                    .filter(r -> r.getName().equals(appName)).findFirst().orElse(null);
            if (report != null) {
                appId = report.getApplicationId().toString();
            } else {
                log.warn("There is no running app named " + appName);
            }
        } catch (IOException | YarnException e) {
            log.warn("Failed to retrieve application id", e);
        }
        return appId;
    }

}
