package com.latticeengines.apps.lp.qbean;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;
import com.google.common.collect.Sets;
import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.yarn.ClusterMetrics;
import com.latticeengines.hadoop.service.EMRCacheService;
import com.latticeengines.yarn.exposed.service.EMREnvService;

public class EMRScalingRunnable implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(EMRScalingRunnable.class);
    private static final int MAX_TASK_NODES = 32;

    private static final int CORE_MB = 24 * 1024;
    private static final int CORE_VCORES = 4;

    private static final int UNIT_MB = 56 * 1024;
    private static final int UNIT_VCORES = 8;

    private static final int MIN_AVAIL_MEM_MB = UNIT_MB;
    private static final int MIN_AVAIL_VCORES = 2;

    private static final long SLOW_START_THRESHOLD = TimeUnit.MINUTES.toMillis(1);
    private static final long HANGING_START_THRESHOLD = TimeUnit.MINUTES.toMillis(10);

    private static final ConcurrentMap<String, AtomicLong> lastScalingUpMap = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, AtomicInteger> scalingDownAttemptMap = new ConcurrentHashMap<>();

    private static final EnumSet<YarnApplicationState> PENDING_APP_STATES = //
            Sets.newEnumSet(Arrays.asList(//
                    YarnApplicationState.NEW, //
                    YarnApplicationState.NEW_SAVING, //
                    YarnApplicationState.SUBMITTED, //
                    YarnApplicationState.ACCEPTED //
            ), YarnApplicationState.class);

    private final String emrCluster;
    private final String clusterId;
    private final EMRService emrService;
    private final EMREnvService emrEnvService;
    private ClusterMetrics metrics = new ClusterMetrics();
    private ReqResource reqResource = new ReqResource();
    private InstanceGroup taskGrp;
    private InstanceGroup coreGrp = null;

    EMRScalingRunnable(String emrCluster, EMRService emrService, EMRCacheService emrCacheService, //
            EMREnvService emrEnvService) {
        this.emrCluster = emrCluster;
        this.emrService = emrService;
        this.emrEnvService = emrEnvService;
        this.clusterId = emrCacheService.getClusterId(emrCluster);
    }

    @Override
    public void run() {
        log.debug("Start processing emr cluster " + emrCluster + " : " + clusterId);

        try {
            RetryTemplate retry = RetryUtils.getRetryTemplate(5);
            metrics = retry.execute(context -> emrEnvService.getClusterMetrics(emrCluster));
        } catch (Exception e) {
            log.error("Failed to retrieve cluster metrics for emr cluster " + emrCluster);
            return;
        }

        try {
            reqResource = getRequestingResources();
        } catch (Exception e) {
            log.error("Failed to retrieve requesting resource submitted to emr cluster "
                    + emrCluster);
            return;
        }

        taskGrp = emrService.getTaskGroup(clusterId);

        if (needToScale()) {
            attemptScale();
        } else {
            resetScalingDownCounter();
        }

        metrics = new ClusterMetrics();
        log.debug("Finished processing emr cluster " + emrCluster);
    }

    private boolean needToScale() {
        String scaleLogPrefix = "Might need to scale " + emrCluster + ": ";
        String noScaleLogPrefix = "No need to scale " + emrCluster + ": ";

        int availableMB = metrics.availableMB;
        int availableVCores = metrics.availableVirtualCores;
        int running = taskGrp.getRunningInstanceCount();
        int requested = taskGrp.getRequestedInstanceCount();

        boolean scale;
        if (reqResource.reqMb > 0 || reqResource.reqVCores > 0) {
            // pending requests
            log.info(scaleLogPrefix + "there are " + reqResource.reqMb + " mb and " //
                    + reqResource.reqVCores + " pending requests.");
            scale = true;
        } else if (availableMB < MIN_AVAIL_MEM_MB) {
            // low mem
            log.info(scaleLogPrefix + "available mb " + availableMB + " is not enough.");
            scale = true;
        } else if (availableVCores < MIN_AVAIL_VCORES) {
            // low vcores
            log.info(scaleLogPrefix + "available vcores " + availableVCores + " is not enough.");
            scale = true;
        } else if (availableMB >= getMaxAvailMemMb() && availableVCores >= getMaxAvailVcores()) {
            // too much mem and vcores
            log.info(scaleLogPrefix + "available mb " + availableMB + " and vcores " //
                    + availableVCores + " are both too high.");
            scale = true;
        } else if (requested < running) {
            // during scaling down, might need to adjust
            log.info(scaleLogPrefix + "scaling down from " + running + " to " //
                    + requested + ", might need to adjust.");
            scale = true;
        } else {
            log.debug(noScaleLogPrefix + "available mb " + availableMB //
                    + " and vcores " + availableVCores + " look healthy.");
            scale = false;
        }
        return scale;
    }

    private void attemptScale() {
        int running = taskGrp.getRunningInstanceCount();
        int requested = taskGrp.getRequestedInstanceCount();
        int target = getTargetTaskNodes();
        if (target != requested) {
            if (target > requested) {
                log.info(String.format("Scale up %s, running=%d, requested=%d, target=%d", //
                        emrCluster, running, requested, target));
                getLastScalingUp().set(System.currentTimeMillis());
                scale(target);
                resetScalingDownCounter();
            } else {
                log.info(String.format(
                        "Scale down %s, attempt=%d, running=%d, requested=%d, target=%d", //
                        emrCluster, getScalingDownAttempt().incrementAndGet(), running, requested,
                        target));
                if (requested <= running && getScalingDownAttempt().get() >= 3) {
                    log.info("Going to scale down " + emrCluster + " from " + requested + " to " + target);
                    // be conservative about terminating machines
                    scale(target);
                    resetScalingDownCounter();
                }
            }
        }
    }

    private void scale(int target) {
        try {
            emrService.scaleTaskGroup(clusterId, target);
        } catch (Exception e) {
            log.error("Failed to scale " + emrCluster + " to " + target, e);
        }
    }

    private ReqResource getRequestingResources() {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(context -> {
            try {
                try (YarnClient yarnClient = emrEnvService.getYarnClient(emrCluster)) {
                    yarnClient.start();
                    List<ApplicationReport> apps = yarnClient.getApplications(PENDING_APP_STATES);
                    return getReqs(apps);
                }
            } catch (IOException | YarnException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private MinNodeResource getMinNodeResource() {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(context -> {
            try {
                try (YarnClient yarnClient = emrEnvService.getYarnClient(emrCluster)) {
                    yarnClient.start();
                    List<NodeReport> nodes = yarnClient.getNodeReports(NodeState.RUNNING);
                    MinNodeResource res = new MinNodeResource();
                    nodes.forEach(node -> {
                        Resource cap = node.getCapability();
                        Resource used = node.getUsed();
                        int availMb = cap.getMemory() - used.getMemory();
                        int availVCores = cap.getVirtualCores() - used.getVirtualCores();
                        res.mb = Math.min(res.mb, availMb);
                        res.vcores = Math.min(res.vcores, availVCores);
                    });
                    return res;
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
                if (now - app.getStartTime() >= SLOW_START_THRESHOLD && used.getMemory() == 0
                        && used.getVirtualCores() == 0) {
                    // no resource usage after SLOW_START_THRESHOLD
                    // must be stuck
                    Resource asked = usageReport.getNeededResources();
                    int mb = asked.getMemory();
                    int vcores = asked.getVirtualCores();
                    reqResource.reqMb += mb;
                    reqResource.reqVCores += vcores;
                    reqResource.maxMb = Math.max(mb, reqResource.maxMb);
                    reqResource.maxVCores = Math.max(vcores, reqResource.maxVCores);
                    if (now - app.getStartTime() >= HANGING_START_THRESHOLD) {
                        reqResource.hangingApps += 1;
                    }
                }
            }
        }
        return reqResource;
    }

    private int getTargetTaskNodes() {
        int targetByMb = determineTargetByMb(reqResource.reqMb);
        int targetByVCores = determineTargetByVCores(reqResource.reqVCores);
        int target = Math.max(targetByMb, targetByVCores);
        if (reqResource.hangingApps > 0) {
            target += reqResource.hangingApps;
        } else if (reqResource.maxMb > 0 || reqResource.maxVCores > 0) {
            int newNodes = determineNewTargetsByMinReq(reqResource.maxMb, reqResource.maxVCores);
            target += newNodes;
        }
        // to be removed to changed to debug
        log.info("Metrics=" + JsonUtils.serialize(metrics) + " Reqs=" + reqResource + " Target="
                + target);
        return Math.min(target, MAX_TASK_NODES);
    }

    private int determineTargetByMb(int req) {
        int coreCount = getCoreCount();
        int avail = metrics.availableMB;
        int total = metrics.totalMB;
        int newTotal = total - avail + req + MIN_AVAIL_MEM_MB;
        int target = (int) Math.max(1,
                Math.ceil((1.0 * (newTotal - CORE_MB * coreCount)) / UNIT_MB));
        log.info(emrCluster + " should have " + target + " TASK nodes, according to mb: " + "total="
                + total + " avail=" + avail + " req=" + req);
        return target;
    }

    private int determineTargetByVCores(int req) {
        int coreCount = getCoreCount();
        int avail = metrics.availableVirtualCores;
        int total = metrics.totalVirtualCores;
        int newTotal = total - avail + req + MIN_AVAIL_VCORES;
        int target = (int) Math.max(1,
                Math.ceil((1.0 * (newTotal - CORE_VCORES * coreCount)) / UNIT_VCORES));
        log.info(emrCluster + " should have " + target + " TASK nodes, according to vcores: "
                + "total=" + total + " avail=" + avail + " req=" + req);
        return target;
    }

    private int determineNewTargetsByMinReq(int mb, int vcores) {
        MinNodeResource minNodeResource = getMinNodeResource();
        // to be removed to changed to debug
        log.info("MinNodeResource=" + minNodeResource);
        if (mb > minNodeResource.mb || vcores > minNodeResource.vcores) {
            // no single node can host the max job
            return 1;
        } else {
            return 0;
        }
    }

    private void resetScalingDownCounter() {
        if (getScalingDownAttempt().get() > 0) {
            log.info("Reset " + emrCluster +" scaling down counter from " + getScalingDownAttempt().get());
            getScalingDownAttempt().set(0);
        }
    }

    private AtomicLong getLastScalingUp() {
        lastScalingUpMap.putIfAbsent(clusterId, new AtomicLong(0L));
        return lastScalingUpMap.get(clusterId);
    }

    private AtomicInteger getScalingDownAttempt() {
        scalingDownAttemptMap.putIfAbsent(clusterId, new AtomicInteger(0));
        return scalingDownAttemptMap.get(clusterId);
    }

    private int getCoreCount() {
        return getCoreGrp().getRunningInstanceCount();
    }

    private int getMaxAvailMemMb() {
        return 2 * UNIT_MB + getCoreCount() * CORE_MB;
    }

    private int getMaxAvailVcores() {
        return 2 * UNIT_VCORES + getCoreCount() * CORE_VCORES;
    }

    private InstanceGroup getCoreGrp() {
        if (coreGrp == null) {
            coreGrp = emrService.getCoreGroup(clusterId);
        }
        return coreGrp;
    }

    private static class MinNodeResource {
        int mb;
        int vcores;

        MinNodeResource() {
            this.mb = Integer.MAX_VALUE;
            this.vcores = Integer.MAX_VALUE;
        }

        @Override
        public String toString() {
            return String.format("[mb=%d, vcores=%d]", mb, vcores);
        }
    }

    private static class ReqResource {
        int reqMb = 0;
        int reqVCores = 0;
        int maxMb = 0;
        int maxVCores = 0;
        int hangingApps = 0;

        @Override
        public String toString() {
            return String.format("[pendingApps=%d, reqMb=%d, reqVCores=%d, maxMb=%d, maxVCores=%d]",
                    hangingApps, reqMb, reqVCores, maxMb, maxVCores);
        }
    }

}
