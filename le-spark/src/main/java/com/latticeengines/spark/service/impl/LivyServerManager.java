package com.latticeengines.spark.service.impl;

import java.util.Iterator;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.ConsulUtils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;

@Component("livyServerManager")
public class LivyServerManager {
    private static final Logger log = LoggerFactory.getLogger(LivyServerManager.class);

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${hadoop.use.defaultlivy:false}")
    private Boolean useDefaultLivy;

    @Inject
    private EMRCacheService emrCacheService;

    @Inject
    private LivyServerManager manager;

    @Value("${hadoop.consul.url}")
    private String consul;

    @Value("${aws.emr.cluster}")
    private String emrClusterName;

    // Define the upper limit for livy sessions on each server
    // for load balance and performance purpose
    private static final int MAX_SESSION_PER_SERVER = 8;

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    // Use a ConcurrentLinkedQueue to maintain server urls
    // Need to consider concurrency situations
    private Queue<String> serverQueue = new ConcurrentLinkedQueue<>();

    public String getLivyHost() {
        return Boolean.TRUE.equals(useEmr) ? (Boolean.TRUE.equals(useDefaultLivy) ? emrCacheService.getLivyUrl()
                : manager.getLivyServerUrl(emrClusterName)) : "http://localhost:8998";
    }

    private String getLivyServerUrl(String emrClusterName) {
        if (!isExternalLivyExists(emrClusterName)) {
            log.info("No external livy server configured for this EMR, use the EMR livy server");
            return emrCacheService.getLivyUrl();
        }

        // If livy server queue is empty, reload it
        if (serverQueue.size() == 0) {
            // Note that newly added server will be loaded here, if any
            if (!populateServerQueue(emrClusterName)) {
                throw new RuntimeException("Reload livy server queue failed");
            }
        }

        // Loop through the livy server queue to find an available livy server
        while (serverQueue.size() != 0) {
            String nextServerUrl = serverQueue.poll();

            if (isLivyServerGoodForUse(nextServerUrl)) {
                log.info("Picked livy server " + nextServerUrl);
                return nextServerUrl;
            } else {
                log.warn("Livy server " + nextServerUrl + " is not reachable...");
            }
        }

        // No external livy server available, throw exception
        throw new RuntimeException("Can't find a good livy server to use...");
    }

    private boolean isExternalLivyExists(String emrClusterName) {
        String consulKey = "emr/" + emrClusterName + "/LivyServerIps";

        return ConsulUtils.isKeyExists(consul, consulKey);
    }

    private synchronized boolean populateServerQueue(String emrClusterName) {
        String consulKey = "emr/" + emrClusterName + "/LivyServerIps";

        String ipStr = ConsulUtils.getValueFromConsul(consul, consulKey);
        if (StringUtils.isBlank(ipStr)) {
            log.error("Livy server ip got from consul is blank.");
            return false;
        }

        String[] serverIps = StringUtils.split(ipStr, '\n');
        log.info("Available livy servers are:");
        // Construct the server queue
        for (String ip : serverIps) {
            log.info("http://" + ip + ":8998");
            serverQueue.offer("http://" + ip + ":8998");
        }

        if (!serverQueue.isEmpty()) {
            // Shuffle the queue to avoid everyone starts with the first one in consul
            int shuffle = new Random(System.currentTimeMillis()).nextInt(serverQueue.size());
            for (int i = 0; i < shuffle; i++) {
                String ip = serverQueue.poll();
                serverQueue.offer(ip);
            }
        }

        return true;
    }

    private boolean isLivyServerGoodForUse(String serverUrl) {
        String url = serverUrl + "/sessions";

        // First, GET all the current sessions on this server
        String resp;
        try {
            resp = restTemplate.getForObject(url, String.class);
        } catch (RestClientException e) {
            log.error("GET livy session failed on " + url, e);
            return false;
        }

        // Loop through the sessions:
        // 1: delete all sessions that are in bad state (save resource)
        // 2: check against max session upper limit
        ObjectMapper om = new ObjectMapper();
        try {
            JsonNode root = om.readTree(resp);
            int total = root.get("total").asInt();
            JsonNode sessions = root.get("sessions");
            Iterator<JsonNode> iter = sessions.elements();
            while (iter.hasNext()) {
                JsonNode json = iter.next();
                String state = json.get("state").asText();
                // Based on our current test results,
                // once livy server has a session in error state, that server is pretty much
                // dead. Skip that server for now
                if (state.equalsIgnoreCase("error")) {
                    log.info(serverUrl + " has session in error state, skip it for now");
                    return false;
                }
                // For other non-active states, try to delete those sessions to save resource
                if (state.equalsIgnoreCase("killed") || state.equalsIgnoreCase("dead")
                        || state.equalsIgnoreCase("success")) {
                    int sessionId = json.get("id").asInt();
                    try {
                        restTemplate.delete(url + sessionId);
                        total--;
                    } catch (Exception e) {
                        // Do nothing, suppress the exceptions coming from delete
                        // as some sessions might already cleaned up by livy during deletion
                        e.printStackTrace();
                    }
                }
            }
            // If remaining sessions already exceed upper limit, return false
            if (total >= MAX_SESSION_PER_SERVER) {
                log.info(serverUrl + " is already loaded");
                return false;
            }
        } catch (Exception e) {
            log.error("Failed while loop through sessions on " + url, e);
            return false;
        }

        return true;
    }
}
