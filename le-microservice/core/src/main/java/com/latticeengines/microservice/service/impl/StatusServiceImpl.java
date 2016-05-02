package com.latticeengines.microservice.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.microservice.service.StatusService;

@Component("statusService")
public class StatusServiceImpl implements StatusService {

    private static final Log log = LogFactory.getLog(StatusServiceImpl.class);

    @Value("${microservices}")
    protected String microservicesStr;

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

    @Value("${microservice.admin.health.url}")
    private String microserviceHealthUrl;

    private RestTemplate restTemplate = new RestTemplate();

    private static Set<String> monitoredApps = new ConcurrentSkipListSet<>();

    private static Map<String, String> healthUrls = new HashMap<>();

    @PostConstruct
    private void postConstruct() {
        healthUrls.put("admin", adminHealthUrl);
        healthUrls.put("pls", plsHealthUrl);
        healthUrls.put("oauth2", oauth2HealthUrl);
        healthUrls.put("playmaker", playmakerHealthUrl);
        healthUrls.put("scoringapi", scoringapiHealthUrl);
        healthUrls.put("microservice", microserviceHealthUrl);
    }

    @Override
    public Map<String, String> moduleStatus() {
        try {
            SSLUtils.turnOffSslChecking();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String[] microservices = microservicesStr.split(",");
        Map<String, String> status = new HashMap<>();
        Boolean overall = true;
        for (String microservice : microservices) {
            try {
                String response = restTemplate.getForObject(String.format("%s/%s/v2/api-docs", microserviceHostport, microservice), String.class);
                if (response.contains("\"swagger\":\"2.0\"")) {
                    status.put(microservice, "OK");
                } else {
                    status.put(microservice, "Unknown api-doc: " + response);
                    overall = false;
                }
            } catch (Exception e) {
                status.put(microservice, ExceptionUtils.getFullStackTrace(e));
                overall = false;
            }
        }

        status.put("Overall", overall ? "OK" : "ERROR");

        return status;
    }

    @Override
    public Map<String, String> appStatus() {
        Map<String, String> statusMap = new HashMap<>();
        for (String app : monitoredApps) {
            String url = healthUrls.get(app);
            statusMap.put(app, isUp(url) ? "OK" : "ERROR");
        }

        for (String app : healthUrls.keySet()) {
            if (!monitoredApps.contains(app)) {
                String url = healthUrls.get(app);
                if (isUp(url)) {
                    log.info("Discovered a new app to monitor: " + app);
                    statusMap.put(app, "OK");
                }
            }
        }

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

}
