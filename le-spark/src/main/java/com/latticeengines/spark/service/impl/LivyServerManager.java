package com.latticeengines.spark.service.impl;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.ConsulUtils;
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

    // use a ConcurrentLinkedQueue to maintain server urls
    // need to consider concurrency situations
    private Queue<String> serverQueue = new ConcurrentLinkedQueue<>();

    public String getLivyHost() {
        return Boolean.TRUE.equals(useEmr) ? manager.getNextLivyServerUrl(emrClusterName) : "http://localhost:8998";
    }

    private String getNextLivyServerUrl(String emrClusterName) {
        if (serverQueue.size() == 0) {
            // reload the server queue. Newly added server will also be loaded here.
            if (populateServerQueue(emrClusterName) == false) {
                // if no external livy server, return the EMR internal livy server
                return emrCacheService.getLivyUrl();
            }
        }

        while (serverQueue.size() != 0) {
            String nextServerUrl = serverQueue.poll();

            if (isLivyServerReachable(nextServerUrl) == true) {
                log.info("Picked livy server " + nextServerUrl);
                return nextServerUrl;
            } else {
                log.warn("Livy server " + nextServerUrl + " is not reachable...");
            }
        }

        // no livy server available, might need to trigger the livy server provision
        // process
        // for now, we will fall back to the EMR internal livy server
        log.warn("Can't find any available external livy server, fall back to EMR internal livy server...");
        return emrCacheService.getLivyUrl();
    }

    private synchronized boolean populateServerQueue(String emrClusterName) {
        String consulKey = "emr/" + emrClusterName + "/LivyServerIps";

        // first check if the key even exists in consul KV store; if not, return right
        // away
        if (ConsulUtils.isKeyExists(consul, consulKey) == false) {
            return false;
        }

        String ipStr = ConsulUtils.getValueFromConsul(consul, consulKey);
        if (StringUtils.isBlank(ipStr)) {
            log.error("Livy server ip got from consul is blank.");
            return false;
        }

        String[] serverIps = StringUtils.split(ipStr, '\n');
        log.info("Available livy servers are:");
        // construct the server queue
        for (String ip : serverIps) {
            log.info("http://" + ip + ":8998");
            serverQueue.offer("http://" + ip + ":8998");
        }

        return true;
    }

    private boolean isLivyServerReachable(String serverUrl) {
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(serverUrl).openConnection();
            connection.setRequestMethod("HEAD");
            int responseCode = connection.getResponseCode();

            return (responseCode == 200) ? true : false;
        } catch (Exception e) {
            log.error("Check server availability failed" + e);
        }

        return false;
    }
}
