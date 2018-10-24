package com.latticeengines.apps.lp.qbean;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
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
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.yarn.ClusterMetrics;
import com.latticeengines.yarn.exposed.service.EMREnvService;

public class EMRScalingRunnable implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(EMRScalingRunnable.class);
    private static final int MAX_TASK_NODES = 32;

    private static final int CORE_MB = 24 * 3 * 1024;
    private static final int CORE_VCORES = 4 * 3;

    private static final int UNIT_MB = 56 * 1024;
    private static final int UNIT_VCORES = 8;

    private static final int MIN_AVAIL_MEM_MB = UNIT_MB;
    private static final int MIN_AVAIL_VCORES = 2;

    private static final int MAX_AVAIL_MEM_MB = 2 * UNIT_MB + CORE_MB;
    private static final int MAX_AVAIL_VCORES = 2 * UNIT_VCORES + CORE_VCORES;

    private static final long SLOW_START_THRESHOLD = TimeUnit.MINUTES.toMillis(2);
    private static final long SCALE_UP_COOLING_PERIOD = TimeUnit.MINUTES.toMillis(10);

    private static final AtomicLong lastScalingUp = new AtomicLong(0);

    private static final EnumSet<YarnApplicationState> PENDING_APP_STATES = //
            Sets.newEnumSet(Arrays.asList(//
                    YarnApplicationState.NEW, //
                    YarnApplicationState.NEW_SAVING, //
                    YarnApplicationState.ACCEPTED //
            ), YarnApplicationState.class);

    private final String emrCluster;
    private final EMRService emrService;
    private final EMREnvService emrEnvService;
    private ClusterMetrics metrics = new ClusterMetrics();

    EMRScalingRunnable(String emrCluster, EMRService emrService, EMREnvService emrEnvService) {
        this.emrCluster = emrCluster;
        this.emrService = emrService;
        this.emrEnvService = emrEnvService;
    }

    @Override
    public void run() {
        log.debug("Start processing emr cluster " + emrCluster);

        try {
            RetryTemplate retry = RetryUtils.getRetryTemplate(5);
            metrics = retry.execute(context -> emrEnvService.getClusterMetrics(emrCluster));
        } catch (Exception e) {
            log.error("Failed to retrieve cluster metrics for emr cluster " + emrCluster);
            return;
        }

        boolean scaled = false;
        if (needToScaleUp()) {
            scaled = scaleUp();
        }

        if (!scaled && needToScaleDown()) {
            scaleDown();
        }

        log.debug("Finished processing emr cluster " + emrCluster);
    }

    private boolean needToScaleUp() {
        String scaleLogPrefix = "Need to scale up " + emrCluster + ": ";
        String noScaleLogPrefix = "No need to scale up " + emrCluster + ": ";

        int availableMB = metrics.availableMB;
        int availableVCores = metrics.availableVirtualCores;

        boolean scale;
        if (availableMB < MIN_AVAIL_MEM_MB) {
            log.info(scaleLogPrefix + "available mb " + availableMB + " is not enough.");
            scale = true;
        } else if (availableVCores < MIN_AVAIL_VCORES) {
            log.info(scaleLogPrefix + "available vcores " + availableVCores + " is not enough.");
            scale = true;
        } else {
            log.debug(noScaleLogPrefix + "have enough available mb " + availableMB //
                    + " and vcores " + availableVCores);
            scale = false;
        }
        return scale;
    }

    private boolean needToScaleDown() {
        String scaleLogPrefix = "Need to scale down " + emrCluster + ": ";
        String noScaleLogPrefix = "No need to scale down " + emrCluster + ": ";

        boolean scale;
        if (System.currentTimeMillis() - lastScalingUp.get() < SCALE_UP_COOLING_PERIOD) {
            log.info(noScaleLogPrefix + "still in cooling period.");
            scale = false;
        } else {
            int availableMB = metrics.availableMB;
            int availableVCores = metrics.availableVirtualCores;
            if (availableMB < MAX_AVAIL_MEM_MB) {
                log.debug(noScaleLogPrefix + "available mb " + availableMB + " is still reasonable.");
                scale = false;
            } else if (availableVCores < MAX_AVAIL_VCORES) {
                log.debug(noScaleLogPrefix + "available vcores " + availableVCores + " is still reasonable.");
                scale = false;
            } else {
                log.info(scaleLogPrefix + "available mb " + availableMB +  " and vcores " //
                        + availableVCores + " are both too high.");
                scale = true;
            }
        }

        return scale;
    }

    private boolean scaleUp() {
        Pair<Integer, Integer> reqs = getRequestingResources();
        if (reqs.getLeft() > metrics.availableMB || reqs.getRight() > metrics.availableVirtualCores) {
            // only scale when insufficient memory
            log.info("Attempt to scale up " + emrCluster);
            int target = getTargetTaskNodes(reqs);
            InstanceGroup taskGrp = emrService.getTaskGroup(emrCluster);
            int running = taskGrp.getRunningInstanceCount();
            int requested = taskGrp.getRequestedInstanceCount();
            log.info(String.format("%s TASK group, running=%d, requested=%d, target=%d", //
                    emrCluster, running, requested, target));
            lastScalingUp.set(System.currentTimeMillis());
            return scale(target);
        } else {
            return false;
        }
    }

    private void scaleDown() {
        log.info("Attempt to scale down " + emrCluster);
        Pair<Integer, Integer> reqs = getRequestingResources();
        int target = getTargetTaskNodes(reqs);
        InstanceGroup taskGrp = emrService.getTaskGroup(emrCluster);
        int running = taskGrp.getRunningInstanceCount();
        int requested = taskGrp.getRequestedInstanceCount();
        log.info(String.format("%s TASK group, running=%d, requested=%d, target=%d", //
                emrCluster, running, requested, target));
        scale(target);
    }

    private boolean scale(int target) {
        try {
            emrService.scaleTaskGroup(emrCluster, target);
            return true;
        } catch (Exception e) {
            log.error("Failed to scale " + emrCluster + " to " + target, e);
            return false;
        }
    }

    private Pair<Integer, Integer> getRequestingResources() {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(context -> {
            try {
                try (YarnClient yarnClient = emrEnvService.getYarnClient(emrCluster)) {
                    yarnClient.start();
                    List<ApplicationReport> apps = yarnClient.getApplications(PENDING_APP_STATES);
                    Pair<Integer, Integer> reqs = Pair.of(0, 0);
                    if (CollectionUtils.isNotEmpty(apps)) {
                        long now = System.currentTimeMillis();
                        for (ApplicationReport app : apps) {
                            ApplicationResourceUsageReport usageReport = app
                                    .getApplicationResourceUsageReport();
                            Resource used = usageReport.getUsedResources();
                            if (now - app.getStartTime() >= SLOW_START_THRESHOLD
                                    && used.getMemory() == 0 && used.getVirtualCores() == 0) {
                                // no resource usage after SLOW_START_THRESHOLD
                                // must be stuck
                                Resource asked = usageReport.getNeededResources();
                                int mb = reqs.getLeft() + asked.getMemory();
                                int vcores = reqs.getRight() + asked.getVirtualCores();
                                if (!isYarnJob(app)) {
                                    // for non yarn job
                                    log.info("Job of type " + app.getApplicationType() + " is asking " //
                                            + mb + " mb and " + vcores + " vcores");
                                }
                                reqs = Pair.of(mb, vcores);
                            }
                        }
                    }
                    return reqs;
                }
            } catch (IOException | YarnException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private boolean isYarnJob(ApplicationReport app) {
        return "YARN".equalsIgnoreCase(app.getApplicationType());
    }

    private int getTargetTaskNodes(Pair<Integer, Integer> reqs) {
        int reqMb = reqs.getLeft();
        int reqVCores = reqs.getRight();
        int targetByMb = determineTargetByMb(reqMb);
        int targetByVCores = determineTargetByVCores(reqVCores);
        return Math.min(Math.max(targetByMb, targetByVCores), MAX_TASK_NODES);
    }

    private int determineTargetByMb(int req) {
        int avail = metrics.availableMB;
        int total = metrics.totalMB;
        int newTotal = total - avail + req + MIN_AVAIL_MEM_MB;
        int target = (int) Math.max(1, Math.ceil((1.0 * (newTotal - CORE_MB)) / UNIT_MB));
        log.info(emrCluster + " should have " + target + " TASK nodes, according to mb: " +
                "total=" + total + " avail=" + avail + " req=" + req);
        return target;
    }

    private int determineTargetByVCores(int req) {
        int avail = metrics.availableVirtualCores;
        int total = metrics.totalVirtualCores;
        int newTotal = total - avail + req + MIN_AVAIL_VCORES;
        int target = (int) Math.max(1, Math.ceil((1.0 * (newTotal - CORE_VCORES)) / UNIT_VCORES));
        log.info(emrCluster + " should have " + target + " TASK nodes, according to vcores: " +
                "total=" + total + " avail=" + avail + " req=" + req);
        return target;
    }

}
