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
import com.latticeengines.domain.exposed.aws.EC2InstanceType;
import com.latticeengines.domain.exposed.yarn.ClusterMetrics;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.yarn.exposed.service.EMREnvService;

public class EMRScalingRunnable implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(EMRScalingRunnable.class);

    private static final long SLOW_START_THRESHOLD = TimeUnit.MINUTES.toMillis(1);
    private static final long HANGING_START_THRESHOLD = TimeUnit.MINUTES.toMillis(5);
    private static final long SCALING_DOWN_COOL_DOWN = TimeUnit.MINUTES.toMillis(45);

    private static final ConcurrentMap<String, AtomicLong> lastScalingUpMap = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, AtomicInteger> scalingDownAttemptMap = new ConcurrentHashMap<>();

    private static final EnumSet<YarnApplicationState> PENDING_APP_STATES = //
            Sets.newEnumSet(Arrays.asList(//
                    YarnApplicationState.NEW, //
                    YarnApplicationState.NEW_SAVING, //
                    YarnApplicationState.SUBMITTED, //
                    YarnApplicationState.ACCEPTED //
            ), YarnApplicationState.class);

    private long coreMb;
    private int coreVCores;
    private long taskMb;
    private int taskVCores;
    private long minAvailMemMb;
    private int minAvailVCores;

    private final String emrCluster;
    private final String clusterId;
    private final EMRService emrService;
    private final EMREnvService emrEnvService;
    private final int minTaskNodes;
    private ClusterMetrics metrics = new ClusterMetrics();
    private ReqResource reqResource = new ReqResource();
    private InstanceGroup taskGrp;
    private InstanceGroup coreGrp = null;

    EMRScalingRunnable(String emrCluster, int minTaskNodes, EMRService emrService, EMRCacheService emrCacheService, //
            EMREnvService emrEnvService) {
        this.emrCluster = emrCluster;
        this.minTaskNodes = minTaskNodes;
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
        taskVCores = getInstanceVCores(taskGrp);
        taskMb = getInstanceMemory(taskGrp);

        // -512 mb to avoid flapping scaling out/in
        // when available resource is right at the threshold
        minAvailMemMb = minTaskNodes * taskMb - 512;
        minAvailVCores = minTaskNodes * taskVCores - 512;

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

        long availableMB = metrics.availableMB;
        int availableVCores = metrics.availableVirtualCores;
        int running = taskGrp.getRunningInstanceCount();
        int requested = taskGrp.getRequestedInstanceCount();

        boolean scale;
        if (reqResource.reqMb > 0 || reqResource.reqVCores > 0) {
            // pending requests
            log.info(scaleLogPrefix + "there are " + reqResource.reqMb + " mb and " //
                    + reqResource.reqVCores + " pending requests.");
            scale = true;
        } else if (availableMB < minAvailMemMb) {
            // low mem
            log.info(scaleLogPrefix + "available mb " + availableMB + " is not enough.");
            scale = true;
        } else if (availableVCores < minAvailVCores) {
            // low vcores
            log.info(scaleLogPrefix + "available vcores " + availableVCores + " is not enough.");
            scale = true;
        } else if (availableMB >= getMaxAvailMemMb() && availableVCores >= getMaxAvailVCores()) {
            // too much mem and vcores
            log.info(scaleLogPrefix + "available mb " + availableMB + " and vcores " //
                    + availableVCores + " are both too high.");
            scale = true;
        } else if (requested < running) {
            // during scaling down, might need to adjust
            log.info(scaleLogPrefix + "scaling in from " + running + " to " //
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
                log.info(String.format("Scale out %s, running=%d, requested=%d, target=%d", //
                        emrCluster, running, requested, target));
                getLastScalingUp().set(System.currentTimeMillis());
                scale(target);
                resetScalingDownCounter();
            } else {
                if (getLastScalingUp().get() + SCALING_DOWN_COOL_DOWN > System.currentTimeMillis()) {
                    log.info("Still in cool down period, won't attempt to scaling in.");
                } else {
                    log.info(String.format(
                            "Scale in %s, attempt=%d, running=%d, requested=%d, target=%d", //
                            emrCluster, getScalingDownAttempt().incrementAndGet(), running, requested,
                            target));
                    if (requested <= running && getScalingDownAttempt().get() >= 3) {
                        log.info("Going to scale in " + emrCluster + " from " + requested + " to " + target);
                        // be conservative about terminating machines
                        scale(target);
                        resetScalingDownCounter();
                    }
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

    private ReqResource getReqs(List<ApplicationReport> apps) {
        ReqResource reqResource = new ReqResource();
        if (CollectionUtils.isNotEmpty(apps)) {
            long now = System.currentTimeMillis();
            for (ApplicationReport app : apps) {
                ApplicationResourceUsageReport usageReport = app
                        .getApplicationResourceUsageReport();
                Resource used = usageReport.getUsedResources();
                Resource asked = usageReport.getNeededResources();
                if (now - app.getStartTime() >= SLOW_START_THRESHOLD && used.getMemorySize() < asked.getMemorySize()
                        && used.getVirtualCores() < asked.getVirtualCores()) {
                    // resource not full-filled after SLOW_START_THRESHOLD
                    // must be stuck
                    long mb = asked.getMemorySize();
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
        target += determineMomentumBuffer();
        if (reqResource.hangingApps > 0) {
            target += reqResource.hangingApps;
        }
        // to be removed to changed to debug
        log.info("Metrics=" + JsonUtils.serialize(metrics) + " Reqs=" + reqResource + " Target="
                + target);
        return Math.min(target, getMaxTaskNodes());
    }

    private int determineTargetByMb(long req) {
        int coreCount = getCoreCount();
        long avail = metrics.availableMB;
        long total = metrics.totalMB;
        long newTotal = total - avail + req + minAvailMemMb;
        int target = (int) Math.max(minTaskNodes,
                Math.ceil((1.0 * (newTotal - coreMb * coreCount)) / taskMb));
        log.info(emrCluster + " should have " + target + " TASK nodes, according to mb: " + "total="
                + total + " avail=" + avail + " req=" + req);
        return target;
    }

    private int determineTargetByVCores(int req) {
        int coreCount = getCoreCount();
        int avail = metrics.availableVirtualCores;
        int total = metrics.totalVirtualCores;
        int newTotal = total - avail + req + minAvailVCores;
        int target = (int) Math.max(minTaskNodes,
                Math.ceil((1.0 * (newTotal - coreVCores * coreCount)) / taskVCores));
        log.info(emrCluster + " should have " + target + " TASK nodes, according to vcores: "
                + "total=" + total + " avail=" + avail + " req=" + req);
        return target;
    }

    private int determineMomentumBuffer() {
        long usedMb = metrics.totalMB - metrics.availableMB;
        double buffer1 = 0.5 * usedMb / taskMb;
        int usedVCores = metrics.totalVirtualCores - metrics.availableVirtualCores;
        double buffer2 = 0.5 * usedVCores / taskVCores;
        int buffer = (int) Math.round(Math.max(buffer1, buffer2));
        log.info("Set momentum buffer to " + buffer + " for using " //
                + usedMb + " mb and " + usedVCores + " vcores.");
        return buffer;
    }

    private void resetScalingDownCounter() {
        if (getScalingDownAttempt().get() > 0) {
            log.info("Reset " + emrCluster +" scaling in counter from " + getScalingDownAttempt().get());
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

    private InstanceGroup getCoreGrp() {
        if (coreGrp == null) {
            coreGrp = emrService.getCoreGroup(clusterId);
            coreMb = getInstanceMemory(coreGrp);
            coreVCores = getInstanceVCores(coreGrp);
        }
        return coreGrp;
    }

    private int getMaxTaskNodes() {
        return getCoreCount() * 4;
    }

    private int getCoreCount() {
        return getCoreGrp().getRunningInstanceCount();
    }

    private long getMaxAvailMemMb() {
        return (long) Math.floor((minTaskNodes + 0.5) * taskMb + getCoreCount() * coreMb);
    }

    private int getMaxAvailVCores() {
        return (int) Math.floor((minTaskNodes + 0.5) * taskVCores + getCoreCount() * coreVCores);
    }

    private int getInstanceVCores(InstanceGroup grp) {
        EC2InstanceType instanceType = EC2InstanceType.fromName(grp.getInstanceType());
        return instanceType.getvCores();
    }

    private long getInstanceMemory(InstanceGroup grp) {
        EC2InstanceType instanceType = EC2InstanceType.fromName(grp.getInstanceType());
        long mem = (long) (instanceType.getMemGb() - 8) * 1024;
        if (mem < 1024 * 4) {
            throw new IllegalArgumentException("Instance type " + grp.getInstanceType() + " is too small");
        }
        return mem;
    }

    private static class SingleNodeResource {
        long mb = 0L;
        int vcores = 0;

        @Override
        public String toString() {
            return String.format("[mb=%d, vcores=%d]", mb, vcores);
        }
    }

    private static class ReqResource {
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
