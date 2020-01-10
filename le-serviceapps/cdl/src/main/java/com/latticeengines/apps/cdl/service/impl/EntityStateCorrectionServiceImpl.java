package com.latticeengines.apps.cdl.service.impl;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.EntityStateCorrectionService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("entityStateCorrectionService")
public class EntityStateCorrectionServiceImpl implements EntityStateCorrectionService {
    private static final Logger log = LoggerFactory.getLogger(EntityStateCorrectionServiceImpl.class);

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private WorkflowProxy workflowProxy;

    public EntityStateCorrectionServiceImpl() {

    }

    @Override
    public Boolean execute() {
        attemptPlayLaunchStateCorrection();
        attemptPlayLaunchChannelStateCorrection();
        attemptModelIterationStateCorrection();
        return true;
    }

    private void attemptPlayLaunchChannelStateCorrection() {
        // TODO: PLS-14138
    }

    private void attemptModelIterationStateCorrection() {
        // TODO: PLS-14138
    }

    private void attemptPlayLaunchStateCorrection() {
        List<PlayLaunch> launchingPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Launching, null);

        log.info("Found " + launchingPlayLaunches.size() + " launches currently launching");

        // Attempt to clear out stuck/failed jobs
        if (launchingPlayLaunches.stream().anyMatch(l -> l.getCreated().toInstant().atOffset(ZoneOffset.UTC)
                .isBefore(Instant.now().atOffset(ZoneOffset.UTC).minusHours(24)))) {
            log.info("Found "
                    + launchingPlayLaunches.stream()
                            .filter(l -> l.getCreated().toInstant().atOffset(ZoneOffset.UTC)
                                    .isBefore(Instant.now().atOffset(ZoneOffset.UTC).minusHours(24)))
                            .count()
                    + " launches running for more than 24 hours. Attempting Launch status cleanup");
            clearStuckOrFailedLaunches(launchingPlayLaunches);
        }
    }

    private boolean clearStuckOrFailedLaunches(List<PlayLaunch> launchingPlayLaunches) {
        boolean launchesProcessed = false;

        // Case 1) Launches are Launching state but have no applicationId -> set
        // pl to cancelled
        List<PlayLaunch> launchesToProcess = launchingPlayLaunches.stream()
                .filter(launch -> StringUtils.isBlank(launch.getApplicationId())).collect(Collectors.toList());
        if (launchesToProcess.size() > 0) {
            log.info(launchesToProcess.size()
                    + " PlayLaunches found with state Launching but no ApplicationId assigned, marking them Cancelled");
            launchesProcessed = processInvalidLaunches(launchesToProcess, LaunchState.Canceled);
        }

        // Case 2) Launches have an ApplicationId but applicationId doesn't
        // exist in Workflowjob -> set pl to cancelled
        launchesToProcess = launchingPlayLaunches.stream()
                .filter(launch -> StringUtils.isNotBlank(launch.getApplicationId()))
                .filter(launch -> workflowProxy.getWorkflowJobFromApplicationId(launch.getApplicationId()) == null)
                .collect(Collectors.toList());
        if (launchesToProcess.size() > 0) {
            log.info(launchesToProcess.size()
                    + " PlayLaunches found with state Launching but orphan ApplicationIds assigned, marking them Cancelled");
            launchesProcessed = processInvalidLaunches(launchesToProcess, LaunchState.Canceled);
        }

        // Case 3) Launches are Launching State but WorkflowJob has terminated
        // -> set pl to Failed
        launchesToProcess = launchingPlayLaunches.stream()
                .filter(launch -> StringUtils.isNotBlank(launch.getApplicationId())).filter(launch -> {
                    Job job = workflowProxy.getWorkflowJobFromApplicationId(launch.getApplicationId());
                    if (job != null && job.getJobStatus().isTerminated()) {
                        launch.setLaunchState(LaunchState.translateFromJobStatus(job.getJobStatus()));
                        return true;
                    }
                    return false;
                }).collect(Collectors.toList());

        if (launchesToProcess.size() > 0) {
            log.info(launchesToProcess.size()
                    + " PlayLaunches found with state Launching but a terminated workflowjob status");
            launchesProcessed = processInvalidLaunches(launchesToProcess, null);
        }

        // Case 4) Launches are queued but the play has been soft deleted ->
        // mark pl to Cancelled
        launchesToProcess = launchingPlayLaunches.stream().filter(launch -> launch.getPlay().getDeleted())
                .collect(Collectors.toList());
        if (launchesToProcess.size() > 0) {
            log.info(launchesToProcess.size()
                    + " PlayLaunches found where the Play was deleted but PlayLaunch was not, marking them Cancelled");
            launchesProcessed = processInvalidLaunches(launchesToProcess, LaunchState.Canceled);
        }
        return launchesProcessed;
    }

    private boolean processInvalidLaunches(List<PlayLaunch> launchesToProcess, LaunchState launchState) {
        if (launchesToProcess.size() > 0) {
            launchesToProcess.forEach(l -> {
                if (launchState != null) {
                    l.setLaunchState(launchState);
                }
                MultiTenantContext.setTenant(l.getTenant());
                playLaunchService.update(l);
                MultiTenantContext.clearTenant();
            });
            return true;
        }
        return false;
    }

}
