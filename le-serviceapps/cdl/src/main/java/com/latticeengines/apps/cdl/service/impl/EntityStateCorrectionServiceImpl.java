package com.latticeengines.apps.cdl.service.impl;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.EntityStateCorrectionService;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.pls.EmailProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("entityStateCorrectionService")
public class EntityStateCorrectionServiceImpl implements EntityStateCorrectionService {
    private static final Logger log = LoggerFactory.getLogger(EntityStateCorrectionServiceImpl.class);

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private EmailProxy emailProxy;

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
        List<PlayLaunch> launchingPlayLaunches = playLaunchService.findByStateAcrossTenants(LaunchState.Launching, null);

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
            try {
                clearStuckOrFailedLaunches(launchingPlayLaunches);
            } catch (Exception e) {
                log.error("Failed to clear stuck launches", e);
            }
        }
    }

    private boolean clearStuckOrFailedLaunches(List<PlayLaunch> launchingPlayLaunches) {
        boolean launchesProcessed = false;

        // Case 1) Launches are Launching state but have no WorkflowJob id's for launch
        // or delta workflows -> set
        // pl to cancelled
        List<PlayLaunch> launchesToProcess = launchingPlayLaunches.stream()
                .filter(launch -> launch.getLaunchWorkflowId() == null && launch.getParentDeltaWorkflowId() == null)
                .collect(Collectors.toList());
        if (launchesToProcess.size() > 0) {
            log.info(launchesToProcess.size()
                    + " PlayLaunches found with state Launching but no Launch workflows assigned, marking them Cancelled");
            launchesProcessed = processInvalidLaunches(launchesToProcess, LaunchState.Canceled);
        }

        // Case 2.a) Launches have an DeltaWorkflowId set no Workflowjob exists for that
        // pid -> set pl to cancelled
        launchesToProcess = launchingPlayLaunches.stream().filter(launch -> launch.getParentDeltaWorkflowId() != null)
                .filter(launch -> workflowProxy.getWorkflowJobByWorkflowJobPid(
                        CustomerSpace.shortenCustomerSpace(launch.getTenant().getId()),
                        launch.getParentDeltaWorkflowId()) == null)
                .collect(Collectors.toList());
        if (launchesToProcess.size() > 0) {
            log.info(launchesToProcess.size()
                    + " PlayLaunches found with state Launching but orphan ParentDeltaWorkflowId assigned, marking them Cancelled");
            launchesProcessed = processInvalidLaunches(launchesToProcess, LaunchState.Canceled);
        }

        // Case 2.b) Launches have an LaunchWorkflowId set no Workflowjob exists for
        // that pid -> set pl to cancelled
        launchesToProcess = launchingPlayLaunches.stream().filter(launch -> launch.getLaunchWorkflowId() != null)
                .filter(launch -> workflowProxy.getWorkflowJobByWorkflowJobPid(
                        CustomerSpace.shortenCustomerSpace(launch.getTenant().getId()),
                        launch.getLaunchWorkflowId()) == null)
                .collect(Collectors.toList());
        if (launchesToProcess.size() > 0) {
            log.info(launchesToProcess.size()
                    + " PlayLaunches found with state Launching but orphan LaunchWorkflowId assigned, marking them Cancelled");
            launchesProcessed = processInvalidLaunches(launchesToProcess, LaunchState.Canceled);
        }

        // Case 3.a) Launches are Launching State but Delta WorkflowJob has terminated
        // -> set pl to Failed
        launchesToProcess = launchingPlayLaunches.stream().filter(launch -> launch.getParentDeltaWorkflowId() != null)
                .filter(launch -> {
                    Job job = workflowProxy.getJobByWorkflowJobPid(
                            CustomerSpace.shortenCustomerSpace(launch.getTenant().getId()),
                            launch.getParentDeltaWorkflowId());
                    if (job != null && job.getJobStatus().isTerminated()) {
                        launch.setLaunchState(LaunchState.translateFromJobStatus(job.getJobStatus()));
                        return true;
                    }
                    return false;
                }).collect(Collectors.toList());

        if (launchesToProcess.size() > 0) {
            log.info(launchesToProcess.size()
                    + " PlayLaunches found with state Launching but a terminated delta workflowjob status");
            launchesProcessed = processInvalidLaunches(launchesToProcess, null);
        }

        // Case 3.b) Launches are Launching State but Launch WorkflowJob has terminated
        // -> set pl to Failed
        launchesToProcess = launchingPlayLaunches.stream().filter(launch -> launch.getLaunchWorkflowId() != null)
                .filter(launch -> {
                    Job job = workflowProxy.getJobByWorkflowJobPid(
                            CustomerSpace.shortenCustomerSpace(launch.getTenant().getId()),
                            launch.getLaunchWorkflowId());
                    if (job != null && job.getJobStatus().isTerminated()) {
                        launch.setLaunchState(LaunchState.translateFromJobStatus(job.getJobStatus()));
                        return true;
                    }
                    return false;
                }).collect(Collectors.toList());

        if (launchesToProcess.size() > 0) {
            log.info(launchesToProcess.size()
                    + " PlayLaunches found with state Launching but a terminated launch workflowjob status");
            launchesProcessed = processInvalidLaunches(launchesToProcess, null);
        }

        // Case 4) Launches are launching but the play has been soft deleted ->
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
            launchesToProcess.forEach(launch -> {
                if (launchState != null) {
                    launch.setLaunchState(launchState);
                }
                MultiTenantContext.setTenant(launch.getTenant());
                recoverLaunchUniverse(launch.getLaunchId());
                playLaunchService.update(launch);
                PlayLaunchChannel channel = playLaunchService.findPlayLaunchChannelByLaunchId(launch.getId());
                try {
                    emailProxy.sendPlayLaunchErrorEmail(launch.getTenant().getId(),
                            channel.getUpdatedBy(), launch);
                } catch (Exception e) {
                    log.error("Can not send play launch failed email: " + e.getMessage());
                }
                MultiTenantContext.clearTenant();
            });
            return true;
        }
        return false;
    }

    private void recoverLaunchUniverse(String launchId) {
        PlayLaunchChannel channel = playLaunchService.findPlayLaunchChannelByLaunchId(launchId);
        playLaunchChannelService.recoverLaunchUniverse(channel);
    }
}
