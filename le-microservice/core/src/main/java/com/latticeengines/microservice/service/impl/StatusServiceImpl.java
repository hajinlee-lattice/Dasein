package com.latticeengines.microservice.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.microservice.service.StatusService;

@Component("statusService")
public class StatusServiceImpl implements StatusService {

    private static final Logger log = LoggerFactory.getLogger(StatusServiceImpl.class);

    @Value("${microservice.modules}")
    protected String microservicesStr;

    @Value("${microservice.apps}")
    protected String initApps;

    @Value("${microservice.rest.endpoint.hostport}")
    private String microserviceHostport;

    @Value("${microservice.admin.health.url}")
    private String adminHealthUrl;

    @Value("${microservice.pls.health.url}")
    private String plsHealthUrl;

    @Value("${microservice.oauth2.health.url}")
    private String oauth2HealthUrl;

    @Value("${microservice.playmaker.health.url}")
    private String playmakerHealthUrl;

    @Value("${microservice.scoringapi.health.url}")
    private String scoringapiHealthUrl;

    @Value("${microservice.matchapi.health.url}")
    private String matchapiHealthUrl;

    @Value("${microservice.microservice.health.url}")
    private String microserviceHealthUrl;

    @Value("${microservice.ulysses.health.url}")
    private String ulyssesHealthUrl;

    private RestTemplate restTemplate = getRestTemplate();

    private static Set<String> monitoredApps = new ConcurrentSkipListSet<>();

    private static Map<String, String> healthUrls = new HashMap<>();

    @PostConstruct
    private void postConstruct() {
        healthUrls.put("admin", adminHealthUrl);
        healthUrls.put("pls", plsHealthUrl);
        healthUrls.put("scoringapi", scoringapiHealthUrl);
        healthUrls.put("matchapi", matchapiHealthUrl);
        healthUrls.put("microservice", microserviceHealthUrl);
        healthUrls.put("ulysses", ulyssesHealthUrl);

        if (StringUtils.isNotEmpty(initApps)) {
            Collections.addAll(monitoredApps, initApps.split(","));
        }
    }

    @Override
    public Map<String, String> moduleStatus() {
        String[] microservices = microservicesStr.split(",");
        Map<String, String> status = new HashMap<>();
        Boolean overall = true;
        for (String microservice : microservices) {
            try {
                String response = restTemplate.getForObject(
                        String.format("%s/%s/v2/api-docs", microserviceHostport, microservice), String.class);
                if (response.contains("\"swagger\":\"2.0\"")) {
                    status.put(microservice, "OK");
                } else {
                    status.put(microservice, "Unknown api-doc: " + response);
                    overall = false;
                }
            } catch (Exception e) {
                status.put(microservice, ExceptionUtils.getStackTrace(e));
                overall = false;
            }
        }

        status.put("Overall", overall ? "OK" : "ERROR");
        return status;
    }

    @Override
    public Map<String, String> appStatus() {
        Map<String, String> statusMap = new HashMap<>();
        LogManager.getLogger(RestTemplate.class).setLevel(Level.FATAL);

        for (String app : monitoredApps) {
            String url = healthUrls.get(app);
            boolean thisAppIsUp = isUp(url);
            statusMap.put(app, thisAppIsUp ? "OK" : "ERROR");
            if (!thisAppIsUp) {
                break;
            }
        }

        if (!statusMap.containsValue("ERROR")) {
            for (String app : healthUrls.keySet()) {
                if (!monitoredApps.contains(app)) {
                    String url = healthUrls.get(app);
                    if (isUp(url)) {
                        log.info("Discovered a new app to monitor: " + app);
                        statusMap.put(app, "OK");
                        monitoredApps.add(app);
                    }
                }
            }
        }

        LogManager.getLogger(RestTemplate.class).setLevel(Level.INFO);
        statusMap.put("Overall", statusMap.containsValue("ERROR") ? "ERROR" : "OK");
        return statusMap;
    }

    @Override
    public void unhookApp(String app) {
        if (monitoredApps.contains(app)) {
            monitoredApps.remove(app);
        }
    }

    private Boolean isUp(String url) {
        try {
            StatusDocument doc = restTemplate.getForObject(url, StatusDocument.class);
            return StatusDocument.ONLINE.equals(doc.getStatus()) || StatusDocument.UP.equals(doc.getStatus());
        } catch (Exception e) {
            return false;
        }
    }

    private static RestTemplate getRestTemplate() {
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory> create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory()) //
                .register("https", SSLUtils.SSL_BLIND_SOCKET_FACTORY) //
                .build();
        PoolingHttpClientConnectionManager pool = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        pool.setDefaultMaxPerRoute(2);
        pool.setMaxTotal(32);
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory( //
                HttpClientBuilder.create() //
                        .setConnectionManager(pool) //
                        .build());
        requestFactory.setReadTimeout(5000);
        requestFactory.setConnectTimeout(10000);
        return new RestTemplate(requestFactory);
    }

}
