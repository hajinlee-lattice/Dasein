package com.latticeengines.apps.cdl.service.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.CampaignLaunchTriggerService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("campaignLaunchTriggerService")
public class CampaignLaunchTriggerServiceImpl extends BaseRestApiProxy implements CampaignLaunchTriggerService {
    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchTriggerServiceImpl.class);

    @Value("${cdl.campaignLaunch.maximum.job.count}")
    private Long maxToLaunch;

    @Value("common.internal.app.url")
    private String internalAppUrl;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private WorkflowProxy workflowProxy;

    private final String baseLaunchUrlPrefix = "/customerspaces/{customerSpace}/plays/{playId}/launches/{launchId}";
    private final String kickoffLaunchPrefix = baseLaunchUrlPrefix + "/kickoff-launch";

    public CampaignLaunchTriggerServiceImpl() {
        super(PropertyUtils.getProperty("common.internal.app.url"), "cdl");
    }

    @Override
    public Boolean triggerQueuedLaunches() {
        if (StringUtils.isBlank(internalAppUrl)) {
            log.warn("Common internal app url not found, ignoring cdlCampaignLaunchJob job");
            return false;
        }

        List<PlayLaunch> queuedPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Queued,
                maxToLaunch);
        if (CollectionUtils.isEmpty(queuedPlayLaunches)) {
            log.info("No Queued Launches found");
            return true;
        }
        log.info("Found " + queuedPlayLaunches.size() + " queued launches");

        List<PlayLaunch> launchingPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Launching, null);

        // Attempt to clear out stuck/failed jobs
        if (CollectionUtils.isNotEmpty(launchingPlayLaunches) && launchingPlayLaunches.size() > maxToLaunch
                && clearStuckOrFailedLaunches(launchingPlayLaunches)) {
            launchingPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Launching, null);
        }

        if (launchingPlayLaunches.size() > maxToLaunch) {
            log.info(String.format("%s Launch jobs are currently running, no new jobs can be kicked off ",
                    launchingPlayLaunches.size()));
            return true;
        }
        Set<Pair<Play, PlayLaunchChannel>> currentlyLaunchingPlayAndChannels = launchingPlayLaunches.stream()
                .map(launch -> Pair.of(launch.getPlay(), launch.getPlayLaunchChannel()))
                .collect(Collectors.toSet());

        Iterator<PlayLaunch> iterator = queuedPlayLaunches.iterator();
        int i = launchingPlayLaunches.size();

        while (i < maxToLaunch && iterator.hasNext()) {
            PlayLaunch launch = iterator.next();
            if (currentlyLaunchingPlayAndChannels
                    .contains(Pair.of(launch.getPlay(), launch.getPlayLaunchChannel()))) {
                continue;
            }

            String url = constructUrl(kickoffLaunchPrefix,
                    CustomerSpace.parse(launch.getTenant().getId()).getTenantId(), launch.getPlay().getName(),
                    launch.getId());
            try {
                String appId = post("Kicking off Play Launch Workflow for Play: " + launch.getPlay().getName(), url,
                        null, String.class);
                launch.setApplicationId(appId);
                launch.setLaunchState(LaunchState.Launching);
                playLaunchService.update(launch);
            } catch (Exception e) {
                log.error("Failed to kick off Play launch", e);
            }
            i++;
        }
        return true;
    }

    private boolean clearStuckOrFailedLaunches(List<PlayLaunch> launchingPlayLaunches) {
        boolean launchesProcessed = false;

        // Case 1) Launches are Launching state but have no applicationId -> set pl to cancelled
        List<PlayLaunch> launchesToProcess = launchingPlayLaunches.stream()
                .filter(launch -> StringUtils.isBlank(launch.getApplicationId())).collect(Collectors.toList());
        if (launchesToProcess.size() > 0) {
            log.info(launchesToProcess.size()
                    + " PlayLaunches found with state Launching but no ApplicationId assigned, marking them Cancelled");
            launchesProcessed = processInvalidLaunches(launchesToProcess, LaunchState.Canceled);
        }

        // Case 2) Launches have an ApplicationId but applicationId doesn't exist in Workflowjob -> set pl to cancelled
        launchesToProcess = launchingPlayLaunches.stream()
                .filter(launch -> StringUtils.isNotBlank(launch.getApplicationId()))
                .filter(launch -> workflowProxy.getWorkflowJobFromApplicationId(launch.getApplicationId()) == null)
                .collect(Collectors.toList());
        if (launchesToProcess.size() > 0) {
            log.info(launchesToProcess.size()
                    + " PlayLaunches found with state Launching but orphan ApplicationIds assigned, marking them Cancelled");
            launchesProcessed = processInvalidLaunches(launchesToProcess, LaunchState.Canceled);
        }

        // Case 3) Launches are Launching State but WorkflowJob has terminated -> set pl to Failed
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

        // Case 4) Launches are queued but the play has been soft deleted -> mark pl to Cancelled
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
                playLaunchService.update(l);
            });
            return true;
        }
        return false;
    }

}
