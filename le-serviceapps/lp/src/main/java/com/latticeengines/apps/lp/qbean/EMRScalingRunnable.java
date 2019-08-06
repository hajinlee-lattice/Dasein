package com.latticeengines.apps.lp.qbean;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;
import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.yarn.ClusterMetrics;
import com.latticeengines.yarn.exposed.service.EMREnvService;

public class EMRScalingRunnable implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(EMRScalingRunnable.class);

    private static final int MAX_SCALE_IN_SIZE = 8;
    private static final int MAX_SCALE_IN_ATTEMPTS = 3;
    private static final int MAX_SCALE_OUT_ATTEMPTS = 5;
    private static final long SCALE_IN_COOL_DOWN_AFTER_SCALING_OUT = TimeUnit.MINUTES.toMillis(40);
    private static final long SLOW_DECOMMISSION_THRESHOLD = TimeUnit.MINUTES.toMillis(10);

    private static final ConcurrentMap<String, ScaleCounter> scalingCounterMap = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, YarnTracker> yarnTrackerMap = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, EMRTracker> emrTrackerMap = new ConcurrentHashMap<>();

    private long coreMb;
    private int coreVCores;
    private long taskMb;
    private int taskVCores;
    private int runningCore;
    private long minAvailMemMb;
    private int minAvailVCores;

    private final String emrCluster;
    private final String clusterId;
    private final EMRService emrService;
    private final EMREnvService emrEnvService;
    private final int maxTaskCoreRatio;
    private ClusterMetrics metrics = new ClusterMetrics();
    private YarnTracker.ReqResource reqResource = new YarnTracker.ReqResource();

    EMRScalingRunnable(String emrCluster, String clusterId, int maxTaskCoreRatio, //
                       EMRService emrService, EMREnvService emrEnvService) {
        this.emrCluster = emrCluster;
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
            throw e;
        }

        try {
            reqResource = getYarnTracker().getRequestingResources();
        } catch (Exception e) {
            log.error("Failed to retrieve requesting resource submitted to emr cluster "
                    + emrCluster);
            throw e;
        }

        initializeEmrTracker();

        // always available free resource, until maxed out: 25% core nodes
        minAvailMemMb = getStaticMemBuffer();
        minAvailVCores = getStaticVCoreBuffer();

        if (needToScale()) {
            attemptScale();
        } else {
            getScaleCounter().resetScaleInCounter();
            getScaleCounter().resetScaleOutCounter();
        }

        metrics = new ClusterMetrics();
        log.debug("Finished processing emr cluster " + emrCluster);
    }

    private void initializeEmrTracker() {
        EMRTracker emrTracker = getEMRTracker();
        emrTracker.clear();

        InstanceGroup coreGrp = emrService.getCoreGroup(clusterId);
        if (coreGrp != null) {
            emrTracker.trackCoreGrp(coreGrp);
        } else {
            emrTracker.trackCoreFleet(emrService.getCoreFleet(clusterId));
        }
        coreVCores = emrTracker.getCoreVCores();
        coreMb = emrTracker.getCoreMb();
        runningCore = emrTracker.getRunningCore();
        log.debug(String.format("coreMb=%d, coreVCores=%d, runningCores=%d", coreMb, coreVCores, runningCore));

        InstanceGroup taskGrp = emrService.getTaskGroup(clusterId);
        if (taskGrp != null) {
            emrTracker.trackTaskGrp(taskGrp);
        } else {
            emrTracker.trackTaskFleet(emrService.getTaskFleet(clusterId));
        }
        taskVCores = emrTracker.getTaskVCores();
        taskMb = emrTracker.getTaskMb();
        log.debug(String.format("taskMb=%d, taskVCores=%d", taskMb, taskVCores));
    }

    private boolean needToScale() {
        String scaleLogPrefix = "Might need to scale " + emrCluster + ": ";
        String noScaleLogPrefix = "No need to scale " + emrCluster + ": ";

        long availableMB = metrics.availableMB;
        int availableVCores = metrics.availableVirtualCores;
        int running = getEMRTracker().getRunningTask();
        int requested = getEMRTracker().getRequestedTask();

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
        } else if (availableMB >= 3 * minAvailMemMb && availableVCores >= 3 * minAvailVCores //
                && running > 1) {
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
        int running = getEMRTracker().getRunningTask();
        int requested = getEMRTracker().getRequestedTask();

        if (requested < running) {
            // during scaling in
            if (getYarnTracker().updateDecommissionTime()) {
                log.info("Found stuck decommissioning node, cancel scale in.");
                scale(running);
                getScaleCounter().clearScaleInCounter(running);
                return;
            }
        }

        int target = getTargetTaskNodes();
        if (target > requested) {
            attemptScaleOut(running, requested, target);
        } else if (target < requested) {
            attemptScaleIn(running, requested, target);
        } else {
            log.info(String.format("No need to scale %s, running=%d, requested=%d, target=%d", //
                    emrCluster, running, requested, target));
            getScaleCounter().resetScaleInCounter();
            getScaleCounter().resetScaleOutCounter();
        }
    }

    private void attemptScaleOut(int running, int requested, int target) {
        getScaleCounter().resetScaleInCounter();
        Pair<Integer, Integer> pair = getScaleCounter().incrementScaleOutCounter(target);
        int scaleOutTarget = pair.getLeft();
        int attempts = pair.getValue();
        log.info(String.format("Would like to scale out %s, attempt=%d, running=%d, requested=%d, target=%d, scaleOutTarget=%d", //
                emrCluster, attempts, running, requested, target, scaleOutTarget));
        if (attempts >= MAX_SCALE_OUT_ATTEMPTS) {
            scale(scaleOutTarget);
            getScaleCounter().setLatestScaleOutTime(System.currentTimeMillis());
            getScaleCounter().clearScaleOutCounter(scaleOutTarget);
        }
    }

    private void attemptScaleIn(int running, int requested, int target) {
        getScaleCounter().resetScaleOutCounter();

        Pair<Integer, Integer> pair = getScaleCounter().incrementScaleInCounter(target);
        int scaleInTarget = pair.getLeft();
        int attempts = pair.getValue();
        int idle = getYarnTracker().getIdleTaskNodes();
        log.info(String.format(
                "Would like to scale in %s, attempt=%d, running=%d, requested=%d, target=%d, scaleInTarget=%d, idle=%d", //
                emrCluster, attempts, running, requested, target, scaleInTarget, idle));
        if (getScaleCounter().getLatestScaleOutTime() + SCALE_IN_COOL_DOWN_AFTER_SCALING_OUT //
                > System.currentTimeMillis()) {
            log.info("Still in cool down period, won't attempt to scale in.");
        } else if (running > requested) {
            log.info("Still in the process of scaling in, won't attempt to scale in again.");
        } else if (idle == 0) {
            log.info("There is no idle task nodes, won't attempt to scale in.");
        } else if (attempts >= MAX_SCALE_IN_ATTEMPTS) {
            int nodesToTerminate = Math.min(idle, MAX_SCALE_IN_SIZE);
            scaleInTarget = Math.max(requested - nodesToTerminate, scaleInTarget);
            log.info("Going to scale in " + emrCluster + " from " + requested + " to " + scaleInTarget);
            scale(scaleInTarget);
            getScaleCounter().clearScaleInCounter(scaleInTarget);
        }
    }

    private void scale(int target) {
        try {
            getEMRTracker().scale(target);
        } catch (Exception e) {
            log.error("Failed to scale " + emrCluster + " to " + target, e);
        }
    }

    /**
     * Main scaling logic
     * @return how many task nodes should be running
     */
    private int getTargetTaskNodes() {
        int targetByMb = determineTargetByMb(reqResource.reqMb);
        int targetByVCores = determineTargetByVCores(reqResource.reqVCores);
        int target = Math.max(targetByMb, targetByVCores);
        if (reqResource.hangingApps > 0) {
            target += reqResource.hangingApps;
        }
        return Math.min(target, getMaxTaskNodes());
    }

    private int determineTargetByMb(long req) {
        long avail = metrics.availableMB;
        long total = metrics.totalMB;
        long newTotal = total - avail + req + (2 * minAvailMemMb) - coreMb * runningCore;
        int target = (int) Math.max(1, Math.ceil(1.0 * newTotal / taskMb));
        log.info(emrCluster + " should have " + target + " TASK nodes, according to mb: " + "total="
                + total + " avail=" + avail + " req=" + req +" newTaskTotal=" + newTotal);
        return target;
    }

    private int determineTargetByVCores(int req) {
        int avail = metrics.availableVirtualCores;
        int total = metrics.totalVirtualCores;
        int newTotal = total - avail + req + (2 * minAvailVCores) - coreVCores * runningCore;
        int target = (int) Math.max(1, Math.ceil(1.0 * newTotal / taskVCores));
        log.info(emrCluster + " should have " + target + " TASK nodes, according to vcores: "
                + "total=" + total + " avail=" + avail + " req=" + req+" newTaskTotal=" + newTotal);
        return target;
    }

    private int getStaticVCoreBuffer() {
        return (int) Math.round(0.25 * coreVCores * runningCore / taskVCores);
    }

    private long getStaticMemBuffer() {
        return Math.round(0.25 * coreMb * runningCore / taskMb);
    }

    private ScaleCounter getScaleCounter() {
        scalingCounterMap.putIfAbsent(clusterId, constructScaleCounter());
        return scalingCounterMap.get(clusterId);
    }

    private ScaleCounter constructScaleCounter() {
        PriorityQueue<Pair<Integer, Integer>> scaleInCounter= new PriorityQueue<>(Comparator.comparing(Pair::getLeft));
        PriorityQueue<Pair<Integer, Integer>> scaleOutCounter= new PriorityQueue<>(Comparator.comparing(Pair::getLeft));
        long latestScaleOutTime = System.currentTimeMillis() //
                - SCALE_IN_COOL_DOWN_AFTER_SCALING_OUT + TimeUnit.MINUTES.toMillis(10);
        return new ScaleCounter(emrCluster, scaleInCounter, scaleOutCounter, //
                MAX_SCALE_IN_ATTEMPTS, MAX_SCALE_OUT_ATTEMPTS, latestScaleOutTime);
    }

    private YarnTracker getYarnTracker() {
        yarnTrackerMap.putIfAbsent(clusterId, constructYarnTracker());
        return yarnTrackerMap.get(clusterId);
    }

    private YarnTracker constructYarnTracker() {
        return new YarnTracker(emrCluster, clusterId, emrEnvService, taskMb, taskVCores, SLOW_DECOMMISSION_THRESHOLD);
    }

    private EMRTracker getEMRTracker() {
        emrTrackerMap.putIfAbsent(clusterId, new EMRTracker(clusterId, emrService));
        return emrTrackerMap.get(clusterId);
    }

    private int getMaxTaskNodes() {
        return runningCore * maxTaskCoreRatio;
    }

}
