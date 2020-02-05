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

    @Inject
    private LivyServerManager manager;

    @Inject
    private EMRCacheService emrCacheService;

    @Value("${hadoop.consul.url}")
    private String consul;

    @Value("${aws.emr.cluster}")
    private String emrClusterName;

    // Define the upper limit for livy sessions on each server
    // for load balance and performance purpose
    private static final int MAX_SESSION_PER_SERVER = 10;

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    // Use a ConcurrentLinkedQueue to maintain server urls
    // Need to consider concurrency situations
    private Queue<String> serverQueue = new ConcurrentLinkedQueue<>();

    public String getLivyHost() {
        return Boolean.TRUE.equals(useEmr) ? manager.getNextLivyServerUrl(emrClusterName) : "http://localhost:8998";
    }

    private String getNextLivyServerUrl(String emrClusterName) {
        if (serverQueue.size() == 0) {
            // Reload the server queue. Newly added server will also be loaded here
            if (!populateServerQueue(emrClusterName)) {
                // If no external livy server, return the EMR internal livy server
                return emrCacheService.getLivyUrl();
            }
        }

        while (serverQueue.size() != 0) {
            String nextServerUrl = serverQueue.poll();

            if (isLivyServerGoodForUse(nextServerUrl)) {
                log.info("Picked livy server " + nextServerUrl);
                return nextServerUrl;
            } else {
                log.warn("Livy server " + nextServerUrl + " is not reachable...");
            }
        }

        // No external livy server available, fall back to the EMR internal livy server
        log.warn("Can't find any available external livy server, fall back to EMR internal livy server...");
        return emrCacheService.getLivyUrl();
    }

    private synchronized boolean populateServerQueue(String emrClusterName) {
        String consulKey = "emr/" + emrClusterName + "/LivyServerIps";

        // First check if the key even exists in consul KV store;
        // If not, return right away
        if (!ConsulUtils.isKeyExists(consul, consulKey)) {
            return false;
        }

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
                // For non-active state, delete those sessions to save resource
                if (state.equalsIgnoreCase("error") || state.equalsIgnoreCase("dead")
                        || state.equalsIgnoreCase("success")) {
                    int sessionId = json.get("id").asInt();
                    restTemplate.delete(url + sessionId);
                    total--;
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
