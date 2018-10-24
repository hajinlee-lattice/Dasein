package com.latticeengines.apps.lp.qbean;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
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

    private static final long SLOW_START_THRESHOLD = TimeUnit.MINUTES.toMillis(1);
    private static final long HANGING_START_THRESHOLD = TimeUnit.MINUTES.toMillis(3);
    private static final long SCALE_UP_COOLING_PERIOD = TimeUnit.MINUTES.toMillis(10);

    private static final AtomicLong lastScalingUp = new AtomicLong(0);

    private static final EnumSet<YarnApplicationState> PENDING_APP_STATES = //
            Sets.newEnumSet(Arrays.asList(//
                    YarnApplicationState.NEW, //
                    YarnApplicationState.NEW_SAVING, //
                    YarnApplicationState.SUBMITTED, //
                    YarnApplicationState.ACCEPTED //
            ), YarnApplicationState.class);

    private final String emrCluster;
    private final EMRService emrService;
    private final EMREnvService emrEnvService;
    private ClusterMetrics metrics = new ClusterMetrics();
    private ReqResource reqResource = new ReqResource();

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

        try {
            reqResource = getRequestingResources();
        } catch (Exception e) {
            log.error("Failed to retrieve requesting resource submitted to emr cluster " + emrCluster);
            return;
        }

        boolean scaled = false;
        if (needToScaleUp()) {
            scaled = scaleUp();
        }

        if (!scaled && needToScaleDown()) {
            scaleDown();
        }

        metrics = new ClusterMetrics();
        log.debug("Finished processing emr cluster " + emrCluster);
    }

    private boolean needToScaleUp() {
        String scaleLogPrefix = "Need to scale up " + emrCluster + ": ";
        String noScaleLogPrefix = "No need to scale up " + emrCluster + ": ";

        int availableMB = metrics.availableMB;
        int availableVCores = metrics.availableVirtualCores;

        boolean scale;
        if (reqResource.reqMb > 0 || reqResource.reqVCores > 0) {
            log.info(scaleLogPrefix + "there are " + reqResource.reqMb + " mb and " //
                    + reqResource.reqVCores + " pending requests.");
            scale = true;
        } else if (availableMB < MIN_AVAIL_MEM_MB) {
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
        String scaleLogPrefix = "Might need to scale down " + emrCluster + ": ";
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
        // only scale up when insufficient resource
        InstanceGroup taskGrp = emrService.getTaskGroup(emrCluster);
        int running = taskGrp.getRunningInstanceCount();
        int requested = taskGrp.getRequestedInstanceCount();
        int target = getTargetTaskNodes();
        if (target != requested) {
            log.info(String.format("Scale up %s, running=%d, requested=%d, target=%d", //
                    emrCluster, running, requested, target));
            if (target > requested) {
                lastScalingUp.set(System.currentTimeMillis());
            }
            return scale(target);
        } else {
            // if scaling down, can check if need to adjust
            return requested < running;
        }
    }

    private void scaleDown() {
        if (reqResource.reqMb < metrics.availableMB && reqResource.reqVCores < metrics.availableVirtualCores) {
            // scale when there are excessive resource
            InstanceGroup taskGrp = emrService.getTaskGroup(emrCluster);
            int running = taskGrp.getRunningInstanceCount();
            int requested = taskGrp.getRequestedInstanceCount();
            int target = getTargetTaskNodes();
            if (requested != target) {
                log.info(String.format("Scale down %s, running=%d, requested=%d, target=%d", //
                        emrCluster, running, requested, target));
                scale(target);
            }
        }
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

    private ReqResource getRequestingResources() {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(context -> {
            try {
                try (YarnClient yarnClient = emrEnvService.getYarnClient(emrCluster)) {
                    yarnClient.start();
                    List<ApplicationReport> apps = yarnClient.getApplications(PENDING_APP_STATES);
                    reqResource = new ReqResource();
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
                                int mb = asked.getMemory();
                                int vcores = asked.getVirtualCores();
                                reqResource.reqMb += mb;
                                reqResource.reqVCores += vcores;
                                if (now - app.getStartTime() >= HANGING_START_THRESHOLD) {
                                    reqResource.eagerMb += mb;
                                    reqResource.eagerVCores += vcores;
                                }
                                reqResource.pendingApps += 1;
                            }
                        }
                    }
                    return reqResource;
                }
            } catch (IOException | YarnException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private int getTargetTaskNodes() {
        int targetByMb = determineTargetByMb(reqResource.reqMb - reqResource.eagerMb);
        int targetByVCores = determineTargetByVCores(reqResource.reqVCores - reqResource.eagerVCores);
        int target = Math.max(targetByMb, targetByVCores);
        if (reqResource.eagerMb > 0 || reqResource.eagerVCores > 0) {
            target += determineNewTargetsByReqResource(reqResource.eagerMb, reqResource.eagerVCores);
        }
        log.info("Target=" + target + " Reqs=" + reqResource);
        return Math.min(target, MAX_TASK_NODES);
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

    private int determineNewTargetsByReqResource(int mb, int vcores) {
        int byMb = (int) Math.ceil((1.0 * mb) / UNIT_MB);
        int byVCores = (int) Math.ceil((1.0 * vcores) / UNIT_VCORES);
        int newHosts = Math.max(byMb, byVCores);
        log.info(emrCluster + " should add " + newHosts + " TASK nodes, according to eager requests: " +
                "mb=" + mb + " vcores=" + vcores);
        return newHosts;
    }

    private static class ReqResource {
        int reqMb = 0;
        int reqVCores = 0;
        int eagerMb = 0;
        int eagerVCores = 0;
        int pendingApps = 0;
        @Override
        public String toString() {
            return String.format("[pendingApps=%d, reqMb=%d, reqVCores=%d, eagerMb=%d, eagerVCores=%d]",
                    pendingApps, reqMb, reqVCores, eagerMb, eagerVCores);
        }
    }

}
