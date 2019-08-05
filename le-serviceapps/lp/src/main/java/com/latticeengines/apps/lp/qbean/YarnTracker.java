package com.latticeengines.apps.lp.qbean;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.yarn.exposed.service.EMREnvService;

class YarnTracker {

    private static final Logger log = LoggerFactory.getLogger(YarnTracker.class);
    private static final ConcurrentMap<String, Long> decommissionTimeMap = new ConcurrentHashMap<>();
    private static final EnumSet<YarnApplicationState> PENDING_APP_STATES = //
            Sets.newEnumSet(Arrays.asList(//
                    YarnApplicationState.NEW, //
                    YarnApplicationState.NEW_SAVING, //
                    YarnApplicationState.SUBMITTED, //
                    YarnApplicationState.ACCEPTED //
            ), YarnApplicationState.class);

    private final EMREnvService emrEnvService;
    private final String emrCluster;
    private final String clusterId;

    private final long taskMb;
    private final int taskVCores;

    private final long slowStartThreshold;
    private final long hangingStartThreshold;
    private final long slowDecommissionThreshold;

    YarnTracker(String emrCluster, String clusterId, EMREnvService emrEnvService, //
                long taskMb, int taskVCores, //
                long slowStartThreshold, long hangingStartThreshold, long slowDecommissionThreshold) {
        this.emrCluster = emrCluster;
        this.clusterId = clusterId;
        this.emrEnvService = emrEnvService;

        this.taskMb = taskMb;
        this.taskVCores = taskVCores;

        this.slowStartThreshold = slowStartThreshold;
        this.hangingStartThreshold = hangingStartThreshold;
        this.slowDecommissionThreshold = slowDecommissionThreshold;
    }

    ReqResource getRequestingResources() {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(context -> {
            try {
                try (YarnClient yarnClient = emrEnvService.getYarnClient(clusterId)) {
                    yarnClient.start();
                    List<ApplicationReport> apps = yarnClient.getApplications(PENDING_APP_STATES);
                    return getReqs(apps);
                }
            } catch (IOException | YarnException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private ReqResource getReqs(List<ApplicationReport> apps) {
        ReqResource reqResource = new ReqResource();
        if (CollectionUtils.isNotEmpty(apps)) {
            long now = System.currentTimeMillis();
            for (ApplicationReport app : apps) {
                ApplicationResourceUsageReport usageReport = app
                        .getApplicationResourceUsageReport();
                Resource used = usageReport.getUsedResources();
                Resource asked = usageReport.getNeededResources();
                if (now - app.getStartTime() >= slowStartThreshold && used.getMemorySize() < asked.getMemorySize()
                        && used.getVirtualCores() < asked.getVirtualCores()) {
                    // resource not full-filled after SLOW_START_THRESHOLD
                    // must be stuck
                    long mb = asked.getMemorySize();
                    int vcores = asked.getVirtualCores();
                    reqResource.reqMb += mb;
                    reqResource.reqVCores += vcores;
                    reqResource.maxMb = Math.max(mb, reqResource.maxMb);
                    reqResource.maxVCores = Math.max(vcores, reqResource.maxVCores);
                    if (now - app.getStartTime() >= hangingStartThreshold) {
                        reqResource.hangingApps += 1;
                    }
                }
            }
        }
        return reqResource;
    }

    int getIdleTaskNodes() {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        try {
            return retry.execute(context -> {
                try {
                    try (YarnClient yarnClient = emrEnvService.getYarnClient(clusterId)) {
                        yarnClient.start();
                        List<NodeReport> reports = yarnClient.getNodeReports(NodeState.RUNNING, NodeState.NEW);
                        return reports.stream().mapToInt(report -> {
                            Resource cap = report.getCapability();
                            if (cap.getMemorySize() == taskMb && cap.getVirtualCores() == taskVCores) {
                                // is a task node
                                Resource used = report.getUsed();
                                if (used.getVirtualCores() == 0) {
                                    return 1;
                                }
                            }
                            return 0;
                        }).sum();
                    }
                } catch (IOException | YarnException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            log.error("Failed to check idle task nodes in emr cluster " + emrCluster, e);
            return 0;
        }
    }

    /**
     * update the concurrent map decommissionTimeMap
     * @return return true if there is one node has been decommissioning for too long
     */
    boolean updateDecommissionTime() {
        long now = System.currentTimeMillis();
        Set<String> trackingNodes = new HashSet<>();
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        boolean hasStuckNode;
        try {
            hasStuckNode = retry.execute(context -> {
                try {
                    trackingNodes.clear();
                    trackingNodes.addAll(decommissionTimeMap.keySet());
                    try (YarnClient yarnClient = emrEnvService.getYarnClient(clusterId)) {
                        yarnClient.start();
                        List<NodeReport> reports = yarnClient.getNodeReports(NodeState.DECOMMISSIONING);
                        int indicator = reports.stream().mapToInt(report -> {
                            String address = addressToIp(report.getNodeId().getHost());
                            decommissionTimeMap.putIfAbsent(address, now);
                            trackingNodes.remove(address);
                            long detectedTime = decommissionTimeMap.get(address);
                            long duration = now - detectedTime;
                            if (duration >= slowDecommissionThreshold) {
                                log.info(String.format("Node %s has being decommissioning for %.2f sec", //
                                        address, duration / 1000.));
                                return 1;
                            } else {
                                return 0;
                            }
                        }).max().orElse(0);
                        return indicator > 0;
                    }
                } catch (IOException | YarnException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            log.warn("Failed to check decommissioning task nodes in emr cluster " + emrCluster, e);
            return false;
        }
        trackingNodes.forEach(decommissionTimeMap::remove);
        return hasStuckNode;
    }

    private String addressToIp(String address) {
        String firstPart = address.substring(0, address.indexOf("."));
        return firstPart.replace("ip-", "").replace("-", ".");
    }

    static class ReqResource {
        long reqMb = 0;
        int reqVCores = 0;
        long maxMb = 0;
        int maxVCores = 0;
        int hangingApps = 0;

        @Override
        public String toString() {
            return String.format("[pendingApps=%d, reqMb=%d, reqVCores=%d, maxMb=%d, maxVCores=%d]",
                    hangingApps, reqMb, reqVCores, maxMb, maxVCores);
        }
    }

}
