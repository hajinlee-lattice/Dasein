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

import com.amazonaws.services.elasticmapreduce.model.InstanceFleet;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;
import com.google.common.collect.Sets;
import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.aws.EC2InstanceType;
import com.latticeengines.domain.exposed.yarn.ClusterMetrics;
import com.latticeengines.yarn.exposed.service.EMREnvService;

public class EMRScalingRunnable implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(EMRScalingRunnable.class);

    private static final int MAX_SCALE_IN_SIZE = 8;
    private static final long SLOW_START_THRESHOLD = TimeUnit.MINUTES.toMillis(1);
    private static final long HANGING_START_THRESHOLD = TimeUnit.MINUTES.toMillis(5);
    private static final long SCALE_IN_COOL_DOWN_AFTER_SCALING_OUT = TimeUnit.MINUTES.toMillis(50);

    private static final ConcurrentMap<String, AtomicLong> lastScalingOutMap = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, AtomicInteger> scalingDownAttemptMap = new ConcurrentHashMap<>();

    private static final EnumSet<YarnApplicationState> PENDING_APP_STATES = //
            Sets.newEnumSet(Arrays.asList(//
                    YarnApplicationState.NEW, //
                    YarnApplicationState.NEW_SAVING, //
                    YarnApplicationState.SUBMITTED, //
                    YarnApplicationState.ACCEPTED //
            ), YarnApplicationState.class);

    private static final EnumSet<YarnApplicationState> ACTIVE_APP_STATES = //
            Sets.newEnumSet(Arrays.asList(//
                    YarnApplicationState.NEW, //
                    YarnApplicationState.NEW_SAVING, //
                    YarnApplicationState.SUBMITTED, //
                    YarnApplicationState.ACCEPTED, //
                    YarnApplicationState.RUNNING
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
    private final int maxTaskCoreRatio;
    private ClusterMetrics metrics = new ClusterMetrics();
    private ReqResource reqResource = new ReqResource();

    private InstanceGroup taskGrp;
    private InstanceGroup coreGrp;
    private InstanceFleet taskFleet;
    private InstanceFleet coreFleet;

    EMRScalingRunnable(String emrCluster, String clusterId, int minTaskNodes, int maxTaskCoreRatio, //
                       EMRService emrService, EMREnvService emrEnvService) {
        this.emrCluster = emrCluster;
        this.minTaskNodes = minTaskNodes;
        this.maxTaskCoreRatio = maxTaskCoreRatio;
        this.emrService = emrService;
        this.emrEnvService = emrEnvService;
        this.clusterId = clusterId;
    }

    @Override
    public void run() {
        log.debug("Start processing emr cluster " + emrCluster + " : " + clusterId);

        try {
            RetryTemplate retry = RetryUtils.getRetryTemplate(5);
            metrics = retry.execute(context -> emrEnvService.getClusterMetrics(clusterId));
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
        if (taskGrp != null) {
            taskVCores = getInstanceVCores(taskGrp);
            taskMb = getInstanceMemory(taskGrp);
        } else {
            taskFleet = emrService.getTaskFleet(clusterId);
            taskVCores = getInstanceVCores(taskFleet);
            taskMb = getInstanceMemory(taskFleet);
        }
        log.debug(String.format("taskMb=%d, taskVCores=%d", taskMb, taskVCores));

        // -512 mb and -1 vcore to avoid flapping scaling out/in
        // when available resource is right at the threshold
        minAvailMemMb = minTaskNodes * taskMb - 512;
        minAvailVCores = minTaskNodes * taskVCores - 1;

        if (needToScale()) {
            attemptScale();
        } else {
            resetScaleInCounter();
        }

        metrics = new ClusterMetrics();
        log.debug("Finished processing emr cluster " + emrCluster);
    }

    private boolean needToScale() {
        String scaleLogPrefix = "Might need to scale " + emrCluster + ": ";
        String noScaleLogPrefix = "No need to scale " + emrCluster + ": ";

        long availableMB = metrics.availableMB;
        int availableVCores = metrics.availableVirtualCores;
        int running = getRunningTaskNodes();
        int requested = getRequestedTaskNodes();

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
        int running = getRunningTaskNodes();
        int requested = getRequestedTaskNodes();
        int target = getTargetTaskNodes();
        if (target > requested) {
            // attempt to scale out
            log.info(String.format("Scale out %s, running=%d, requested=%d, target=%d", //
                    emrCluster, running, requested, target));
            scale(target);
            getLastScaleOutTime().set(System.currentTimeMillis());
            resetScaleInCounter();
        } else if (target < requested) {
            // attempt to scale in
            log.info(String.format(
                    "Scale in %s, attempt=%d, running=%d, requested=%d, target=%d", //
                    emrCluster, getScaleInAttempt().incrementAndGet(), running, requested,
                    target));
            if (getLastScaleOutTime().get() + SCALE_IN_COOL_DOWN_AFTER_SCALING_OUT > System.currentTimeMillis()) {
                log.info("Still in cool down period, won't attempt to scale in.");
            } else if (running > requested) {
                log.info("Still in the process of scaling in, won't attempt to scale in again.");
            } else if (hasActiveTezApps()) {
                log.info("Has active TEZ applications, won't attempt to scale in.");
            } else if (getScaleInAttempt().get() >= 5) {
                target = Math.max(requested - MAX_SCALE_IN_SIZE, target);
                log.info("Going to scale in " + emrCluster + " from " + requested + " to " + target);
                scale(target);
                resetScaleInCounter();
            }
        } else {
            log.info(String.format("No need to scale %s, running=%d, requested=%d, target=%d", //
                    emrCluster, running, requested, target));
        }
    }

    private void scale(int target) {
        try {
            if (taskFleet != null) {
                emrService.scaleTaskFleet(clusterId, taskFleet, 0, target);
            } else {
                emrService.scaleTaskGroup(clusterId, taskGrp, target);
            }
        } catch (Exception e) {
            log.error("Failed to scale " + emrCluster + " to " + target, e);
        }
    }

    private ReqResource getRequestingResources() {
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

    private boolean hasActiveTezApps() {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        try {
            return retry.execute(context -> {
                try {
                    try (YarnClient yarnClient = emrEnvService.getYarnClient(clusterId)) {
                        yarnClient.start();
                        List<ApplicationReport> apps = yarnClient.getApplications(ACTIVE_APP_STATES);
                        return apps.stream().anyMatch(appReport ->
                                appReport.getApplicationType().equalsIgnoreCase("TEZ"));
                    }
                } catch (IOException | YarnException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            log.error("Failed to check active TEZ applications in emr cluster " + emrCluster, e);
            return false;
        }
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
        long newTotal = total - avail + req + minAvailMemMb - coreMb * coreCount;
        int target = (int) Math.max(minTaskNodes, Math.ceil(1.0 * newTotal / taskMb));
        log.info(emrCluster + " should have " + target + " TASK nodes, according to mb: " + "total="
                + total + " avail=" + avail + " req=" + req +" newTaskTotal=" + newTotal);
        return target;
    }

    private int determineTargetByVCores(int req) {
        int coreCount = getCoreCount();
        int avail = metrics.availableVirtualCores;
        int total = metrics.totalVirtualCores;
        int newTotal = total - avail + req + minAvailVCores - coreVCores * coreCount;
        int target = (int) Math.max(minTaskNodes, Math.ceil(1.0 * newTotal / taskVCores));
        log.info(emrCluster + " should have " + target + " TASK nodes, according to vcores: "
                + "total=" + total + " avail=" + avail + " req=" + req+" newTaskTotal=" + newTotal);
        return target;
    }

    // use 25% of used resource as buffer
    private int determineMomentumBuffer() {
        long usedMb = metrics.totalMB - metrics.availableMB;
        double buffer1 = 0.25 * usedMb / taskMb;
        int usedVCores = metrics.totalVirtualCores - metrics.availableVirtualCores;
        double buffer2 = 0.25 * usedVCores / taskVCores;
        int buffer = (int) Math.round(Math.max(buffer1, buffer2));
        log.info("Set momentum buffer to " + buffer + " for using " //
                + usedMb + " mb and " + usedVCores + " vcores.");
        return buffer;
    }

    private void resetScaleInCounter() {
        if (getScaleInAttempt().get() > 0) {
            log.info("Reset " + emrCluster +" scaling in counter from " + getScaleInAttempt().get());
            getScaleInAttempt().set(0);
        }
    }

    private AtomicLong getLastScaleOutTime() {
        lastScalingOutMap.putIfAbsent(clusterId, //
                // 10 min cool down
                new AtomicLong(System.currentTimeMillis() - SCALE_IN_COOL_DOWN_AFTER_SCALING_OUT + TimeUnit.MINUTES.toMillis(10)));
        return lastScalingOutMap.get(clusterId);
    }

    private AtomicInteger getScaleInAttempt() {
        scalingDownAttemptMap.putIfAbsent(clusterId, new AtomicInteger(0));
        return scalingDownAttemptMap.get(clusterId);
    }

    private int getMaxTaskNodes() {
        return getCoreCount() * maxTaskCoreRatio;
    }

    private int getCoreCount() {
        if (coreFleet == null && coreGrp == null) {
            coreGrp = emrService.getCoreGroup(clusterId);
            if (coreGrp != null) {
                coreMb = getInstanceMemory(coreGrp);
                coreVCores = getInstanceVCores(coreGrp);
                log.debug(String.format("coreMb=%d, coreVCores=%d", coreMb, coreVCores));
            } else {
                coreFleet = emrService.getCoreFleet(clusterId);
                coreMb = getInstanceMemory(coreFleet);
                coreVCores = getInstanceVCores(coreFleet);
                log.debug(String.format("coreMb=%d, coreVCores=%d", coreMb, coreVCores));

            }
        }
        if (coreFleet != null) {
            return coreFleet.getProvisionedOnDemandCapacity() + coreFleet.getProvisionedSpotCapacity();
        } else {
            return coreGrp.getRunningInstanceCount();
        }
    }

    private long getMaxAvailMemMb() {
        int coreCount = getCoreCount();
        long maxAvailMb = (long) Math.floor((minTaskNodes + 0.5) * taskMb + coreCount * coreMb);
        log.debug(String.format("minTaskNodes=%d, taskMb=%d, coreCount=%d, coreMb=%d: maxAvailMb=%d", //
                minTaskNodes, taskMb, coreCount,coreMb, maxAvailMb));
        return maxAvailMb;
    }

    private int getMaxAvailVCores() {
        int coreCount = getCoreCount();
        int maxAvailVCores =  (int) Math.floor((minTaskNodes + 0.5) * taskVCores + coreCount * coreVCores);
        log.debug(String.format("minTaskNodes=%d, taskVCores=%d, coreCount=%d, coreVCores=%d: maxAvailVCores=%d", //
                minTaskNodes, taskVCores, coreCount, coreVCores, maxAvailVCores));
        return maxAvailVCores;
    }

    private int getInstanceVCores(InstanceFleet fleet) {
        EC2InstanceType instanceType = getFleetInstanceType(fleet);
        return instanceType.getvCores();
    }

    private long getInstanceMemory(InstanceFleet fleet) {
        EC2InstanceType instanceType = getFleetInstanceType(fleet);
        long mem = (long) (instanceType.getMemGb() - 8) * 1024;
        if (mem < 1024 * 4) {
            throw new IllegalArgumentException("Instance type " + instanceType + " is too small");
        }
        return mem;
    }

    private EC2InstanceType getFleetInstanceType(InstanceFleet fleet) {
        String instanceTypeName = fleet.getInstanceTypeSpecifications().get(0).getInstanceType();
        EC2InstanceType instanceType = EC2InstanceType.fromName(instanceTypeName);
        if (instanceType == null) {
            throw new UnsupportedOperationException("Instance type " + instanceTypeName + " is not defined.");
        }
        return instanceType;
    }

    private int getInstanceVCores(InstanceGroup grp) {
        EC2InstanceType instanceType = getGroupInstanceType(grp);
        return instanceType.getvCores();
    }

    private long getInstanceMemory(InstanceGroup grp) {
        EC2InstanceType instanceType = getGroupInstanceType(grp);
        long mem = (long) (instanceType.getMemGb() - 8) * 1024;
        if (mem < 1024 * 4) {
            throw new IllegalArgumentException("Instance type " + grp.getInstanceType() + " is too small");
        }
        return mem;
    }

    private EC2InstanceType getGroupInstanceType(InstanceGroup grp) {
        EC2InstanceType instanceType = EC2InstanceType.fromName(grp.getInstanceType());
        if (instanceType == null) {
            throw new UnsupportedOperationException("Instance type " + instanceType + " is not defined.");
        }
        return instanceType;
    }

    private int getRunningTaskNodes() {
        if (taskFleet != null) {
            return taskFleet.getProvisionedOnDemandCapacity() + taskFleet.getProvisionedSpotCapacity();
        } else {
            return taskGrp.getRunningInstanceCount();
        }
    }

    private int getRequestedTaskNodes() {
        if (taskFleet != null) {
            return taskFleet.getTargetOnDemandCapacity() + taskFleet.getTargetSpotCapacity();
        } else {
            return taskGrp.getRequestedInstanceCount();
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
