package com.latticeengines.apps.lp.qbean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
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
    private static final int MAX_SCALE_IN_ATTEMPTS = 3;
    private static final int MAX_SCALE_OUT_ATTEMPTS = 5;
    private static final long SLOW_START_THRESHOLD = TimeUnit.MINUTES.toMillis(1);
    private static final long HANGING_START_THRESHOLD = TimeUnit.MINUTES.toMillis(5);
    private static final long SCALE_IN_COOL_DOWN_AFTER_SCALING_OUT = TimeUnit.MINUTES.toMillis(40);
    private static final long SLOW_DECOMMISSION_THRESHOLD = TimeUnit.MINUTES.toMillis(10);

    private static final ConcurrentMap<String, AtomicLong> lastScalingOutMap = new ConcurrentHashMap<>();


    // each cluster has a PQ<(target, attempts)> ordered by target.
    private static final ConcurrentMap<String, PriorityQueue<Pair<Integer, Integer>>> scalingInAttemptMap //
            = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, PriorityQueue<Pair<Integer, Integer>>> scalingOutAttemptMap //
            = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, Long> decommissionTimeMap = new ConcurrentHashMap<>();

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
    private final int maxTaskCoreRatio;
    private ClusterMetrics metrics = new ClusterMetrics();
    private ReqResource reqResource = new ReqResource();

    private InstanceGroup taskGrp;
    private InstanceGroup coreGrp;
    private InstanceFleet taskFleet;
    private InstanceFleet coreFleet;

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

        // always available free resource, until maxed out: 25% core nodes
        minAvailMemMb = getStaticMemBuffer();
        minAvailVCores = getStaticVCoreBuffer();

        if (needToScale()) {
            attemptScale();
        } else {
            resetScaleInCounter();
            resetScaleOutCounter();
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
        int running = getRunningTaskNodes();
        int requested = getRequestedTaskNodes();

        if (requested < running) {
            // during scaling in
            if (updateDecommissionTime()) {
                log.info("Found stuck decommissioning node, cancel scale in.");
                scale(running);
                clearScaleInCounter(running);
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
            resetScaleInCounter();
            resetScaleOutCounter();
        }
    }

    private void attemptScaleOut(int running, int requested, int target) {
        resetScaleInCounter();
        Pair<Integer, Integer> pair = incrementScaleOutCounter(target);
        int scaleOutTarget = pair.getLeft();
        int attempts = pair.getValue();
        log.info(String.format("Would like to scale out %s, attempt=%d, running=%d, requested=%d, target=%d, scaleOutTarget=%d", //
                emrCluster, attempts, running, requested, target, scaleOutTarget));
        if (attempts >= MAX_SCALE_OUT_ATTEMPTS) {
            scale(scaleOutTarget);
            getLastScaleOutTime().set(System.currentTimeMillis());
            clearScaleOutCounter(scaleOutTarget);
        }
    }

    private void attemptScaleIn(int running, int requested, int target) {
        resetScaleOutCounter();

        Pair<Integer, Integer> pair = incrementScaleInCounter(target);
        int scaleInTarget = pair.getLeft();
        int attempts = pair.getValue();
        int idle = getIdleTaskNodes();
        log.info(String.format(
                "Would like to scale in %s, attempt=%d, running=%d, requested=%d, target=%d, scaleInTarget=%d, idle=%d", //
                emrCluster, attempts, running, requested, target, scaleInTarget, idle));
        if (getLastScaleOutTime().get() + SCALE_IN_COOL_DOWN_AFTER_SCALING_OUT > System.currentTimeMillis()) {
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
            clearScaleInCounter(scaleInTarget);
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
        long newTotal = total - avail + req + (2 * minAvailMemMb) - coreMb * coreCount;
        int target = (int) Math.max(1, Math.ceil(1.0 * newTotal / taskMb));
        log.info(emrCluster + " should have " + target + " TASK nodes, according to mb: " + "total="
                + total + " avail=" + avail + " req=" + req +" newTaskTotal=" + newTotal);
        return target;
    }

    private int determineTargetByVCores(int req) {
        int coreCount = getCoreCount();
        int avail = metrics.availableVirtualCores;
        int total = metrics.totalVirtualCores;
        int newTotal = total - avail + req + (2 * minAvailVCores) - coreVCores * coreCount;
        int target = (int) Math.max(1, Math.ceil(1.0 * newTotal / taskVCores));
        log.info(emrCluster + " should have " + target + " TASK nodes, according to vcores: "
                + "total=" + total + " avail=" + avail + " req=" + req+" newTaskTotal=" + newTotal);
        return target;
    }

    private int getStaticVCoreBuffer() {
        int coreCount = getCoreCount();
        return (int) Math.round(0.25 * coreVCores * coreCount / taskVCores);
    }

    private long getStaticMemBuffer() {
        int coreCount = getCoreCount();
        return Math.round(0.25 * coreMb * coreCount / taskMb);
    }

    private Pair<Integer, Integer> incrementScaleOutCounter(int target) {
        Pair<Integer, Integer> pair = incrementScaleCounter(-target, true);
        return Pair.of(-pair.getLeft(), pair.getRight());
    }

    private Pair<Integer, Integer> incrementScaleInCounter(int target) {
        return incrementScaleCounter(target, false);
    }

    // pair = (target, attempts)
    private Pair<Integer, Integer> incrementScaleCounter(int target, boolean scaleOut) {
        PriorityQueue<Pair<Integer, Integer>> pq = scaleOut ? getScaleOutAttempt() : getScaleInAttempt();
        insertTargetToPq(pq, target, scaleOut);
        int maxAttempts = scaleOut ? MAX_SCALE_OUT_ATTEMPTS : MAX_SCALE_IN_ATTEMPTS;
        return findBestAttempt(pq, maxAttempts, scaleOut);
    }

    private void insertTargetToPq(PriorityQueue<Pair<Integer, Integer>> pq, int target, boolean scaleOut) {
        // consider scaling in:
        // if target not in PQ, its attempt is the max attempts of all targets below it, + 1
        // for all targets in PQ, if the recorded target above the passed in target, attempt + 1
        // if the recorded target is below, discard that counter (because the streak discontinued)
        int maxAttemptsAboveTarget = 0;
        // remove all pairs having more target
        while (!pq.isEmpty()) {
            Pair<Integer, Integer> head = pq.poll();
            if (head.getKey() <= target) {
                // notice that we also removed the record that key == target
                if (scaleOut) {
                    log.info("Discard {} scale out counter: target={}, attempts={}", //
                            emrCluster, -head.getLeft(), head.getRight());
                } else {
                    log.info("Discard {} scale in counter: target={}, attempts={}", //
                            emrCluster, head.getLeft(), head.getRight());
                }
                maxAttemptsAboveTarget = Math.max(maxAttemptsAboveTarget, head.getRight());
            } else {
                pq.offer(head);
                break;
            }
        }
        // for pairs in pq, increment attempts
        List<Pair<Integer, Integer>> collector = new ArrayList<>();
        while(!pq.isEmpty()) {
            Pair<Integer, Integer> pair = pq.poll();
            int oldTarget = pair.getLeft();
            int oldAttempts = pair.getRight();
            int attempts = oldAttempts + 1;
            collector.add(Pair.of(oldTarget, attempts));
            if (scaleOut) {
                log.info("Increment {} scale out counter: target={}, attempts={}", emrCluster, -oldTarget, attempts);
            } else {
                log.info("Increment {} scale in counter: target={}, attempts={}", emrCluster, oldTarget, attempts);
            }
        }
        // add the new value to pq.
        int attempts = maxAttemptsAboveTarget + 1;
        if (scaleOut) {
            log.info("Add {} scale out counter: target={}, attempts={}", emrCluster, -target, attempts);
        } else {
            log.info("Add {} scale in counter: target={}, attempts={}", emrCluster, target, attempts);
        }
        collector.add(Pair.of(target, attempts));
        collector.forEach(pq::offer);
    }

    private Pair<Integer, Integer> findBestAttempt(PriorityQueue<Pair<Integer, Integer>> pq, //
                                                   int maxAttempts, boolean scaleOut) {
        // 1. find the smallest target that reaches max attempts
        // 2. if not found, return the max attempts for all targets
        Pair<Integer, Integer> finalAttempt = Pair.of(0 ,0);
        List<Pair<Integer, Integer>> collector = new ArrayList<>();
        while (!pq.isEmpty()) {
            Pair<Integer, Integer> head = pq.poll();
            collector.add(head);
            if (head.getRight() >= maxAttempts) {
                if (scaleOut) {
                    log.info("Attempts in {} to scale out to {} has reached maximum.", emrCluster, -head.getLeft());
                } else {
                    log.info("Attempts in {} to scale in to {} has reached maximum.", emrCluster, head.getLeft());
                }
                finalAttempt = head;
                break;
            } else if (head.getRight() > finalAttempt.getRight()) {
                finalAttempt = head;
            }
        }
        collector.forEach(pq::offer);
        return finalAttempt;
    }

    private void resetScaleInCounter() {
        PriorityQueue<Pair<Integer, Integer>> pq = getScaleInAttempt();
        if (!pq.isEmpty()) {
            log.info("Reset " + emrCluster + " scale in counter: " + JsonUtils.serialize(pq));
            pq.clear();
        }
    }

    private void resetScaleOutCounter() {
        PriorityQueue<Pair<Integer, Integer>> pq = getScaleOutAttempt();
        if (!pq.isEmpty()) {
            log.info("Reset " + emrCluster + " scale out counter: " + JsonUtils.serialize(pq));
            pq.clear();
        }
    }

    private void clearScaleInCounter(int scaleInTarget) {
        clearScaleCounter(scaleInTarget, false);
    }

    private void clearScaleOutCounter(int scaleOutTarget) {
        clearScaleCounter(-scaleOutTarget, true);
    }

    private void clearScaleCounter(int scaleTarget, boolean scaleOut) {
        PriorityQueue<Pair<Integer, Integer>> pq = scaleOut ? getScaleOutAttempt() : getScaleInAttempt();
        if (CollectionUtils.isNotEmpty(pq)) {
            // wipe out all pairs having higher target
            List<Pair<Integer, Integer>> collector = new ArrayList<>();
            while (!pq.isEmpty()) {
                Pair<Integer, Integer> head = pq.poll();
                if (head.getKey() >= scaleTarget) {
                    if (scaleOut) {
                        log.info("Discard {} scaling out count: target={}, attempts={}", //
                                emrCluster, -head.getLeft(), head.getRight());
                    } else {
                        log.info("Discard {} scaling in count: target={}, attempts={}", //
                                emrCluster, head.getLeft(), head.getRight());
                    }
                } else {
                    collector.add(head);
                }
            }
            collector.forEach(pq::offer);
        }
    }

    private AtomicLong getLastScaleOutTime() {
        lastScalingOutMap.putIfAbsent(clusterId, //
                // 10 min cool down
                new AtomicLong(System.currentTimeMillis() - SCALE_IN_COOL_DOWN_AFTER_SCALING_OUT + TimeUnit.MINUTES.toMillis(10)));
        return lastScalingOutMap.get(clusterId);
    }

    private PriorityQueue<Pair<Integer, Integer>> getScaleInAttempt() {
        scalingInAttemptMap.putIfAbsent(clusterId, new PriorityQueue<>(Comparator.comparing(Pair::getLeft)));
        return scalingInAttemptMap.get(clusterId);
    }

    private PriorityQueue<Pair<Integer, Integer>> getScaleOutAttempt() {
        scalingOutAttemptMap.putIfAbsent(clusterId, new PriorityQueue<>(Comparator.comparing(Pair::getLeft)));
        return scalingOutAttemptMap.get(clusterId);
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
            throw new UnsupportedOperationException("Instance type " + grp.getInstanceType() + " is not defined.");
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

    private int getIdleTaskNodes() {
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
    private boolean updateDecommissionTime() {
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
                            if (duration >= SLOW_DECOMMISSION_THRESHOLD) {
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

    private static String addressToIp(String address) {
        String firstPart = address.substring(0, address.indexOf("."));
        return firstPart.replace("ip-", "").replace("-", ".");
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
