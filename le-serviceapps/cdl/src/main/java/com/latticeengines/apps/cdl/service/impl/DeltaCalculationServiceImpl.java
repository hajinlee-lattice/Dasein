package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.service.DeltaCalculationService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("deltaCalculationService")
public class DeltaCalculationServiceImpl extends BaseRestApiProxy implements DeltaCalculationService {
    private static final Logger log = LoggerFactory.getLogger(DeltaCalculationServiceImpl.class);

    @Inject
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private BatonService batonService;

    @Value("common.internal.app.url")
    private String internalAppUrl;

    private final String campaignDeltaCalculationUrlPrefix = "/customerspaces/{customerSpace}/plays/{playId}/channels/{channelId}/kickoff-delta-calculation?launchId=";
    private final String campaignLaunchUrlPrefix = "/customerspaces/{customerSpace}/plays/{playId}/channels/{channelId}/launch?is-auto-launch=true&state=";
    private final String setChannelScheduleUrlPrefix = "/customerspaces/{customerSpace}/plays//{playName}/channels/{channelId}/next-scheduled-date";

    public DeltaCalculationServiceImpl() {
        super(PropertyUtils.getProperty("common.internal.app.url"), "cdl");
    }

    @Override
    public Boolean triggerScheduledCampaigns() {
        if (StringUtils.isBlank(internalAppUrl)) {
            log.warn("Common internal app url not found, ignoring this job");
            return false;
        }

        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.getAllValidScheduledChannels();

        List<PlayLaunchChannel> deltaEnabledTenantChannels = channels.stream().filter(c -> batonService
                .isEnabled(CustomerSpace.parse(c.getTenant().getId()), LatticeFeatureFlag.ENABLE_DELTA_CALCULATION))
                .collect(Collectors.toList());

        long inValidDeltaLaunchChannels = channels.stream().filter(c -> c.getLaunchType() == LaunchType.DELTA)
                .filter(c -> !batonService.isEnabled(CustomerSpace.parse(c.getTenant().getId()),
                        LatticeFeatureFlag.ENABLE_DELTA_CALCULATION))
                .count();
        if (inValidDeltaLaunchChannels > 0) {
            log.warn(inValidDeltaLaunchChannels
                    + " channels found scheduled for delta launches, in tenants inactive for delta calculation,"
                    + " these channels will be skipped");
        }

        List<PlayLaunchChannel> deltaDisabledTenantChannels = channels.stream()
                .filter(c -> c.getLaunchType() == LaunchType.FULL)
                .filter(c -> !batonService.isEnabled(CustomerSpace.parse(c.getTenant().getId()),
                        LatticeFeatureFlag.ENABLE_DELTA_CALCULATION))
                .collect(Collectors.toList());

        log.info(String.format(
                "Found %d channels scheduled for launch, Full Launches in Delta Disabled Tenants: %d, Delta Launches in Delta Disabled Tenants: %d,Launches is Delta Enabled tenants: %d",
                channels.size(), deltaDisabledTenantChannels.size(), inValidDeltaLaunchChannels,
                deltaEnabledTenantChannels.size()));

        long successfullyQueuedForDelta = deltaEnabledTenantChannels.stream().map(this::queueNewDeltaCalculationJob)
                .filter(x -> x).count();

        long successfullyQueuedForFull = deltaDisabledTenantChannels.stream().map(this::queueNewFullCampaignLaunch)
                .filter(x -> x).count();

        log.info(String.format(
                "Total Delta Calculation Jobs to be Scheduled: %d, Queued Delta: %d, Queued Full Launches: %d Job Submissions failed: %d",
                channels.size(), //
                successfullyQueuedForDelta, //
                successfullyQueuedForFull, channels.size() - successfullyQueuedForDelta - successfullyQueuedForFull));
        return true;
    }

    private boolean queueNewDeltaCalculationJob(PlayLaunchChannel channel) {
        try {
            Play play = channel.getPlay();
            String url = constructUrl(campaignLaunchUrlPrefix + LaunchState.UnLaunched.name(),
                    CustomerSpace.parse(channel.getTenant().getId()).getTenantId(), play.getName(), channel.getId());

            PlayLaunch launch = post("Creating Unlaunched Campaign Launch", url, null, PlayLaunch.class);
            log.info("Created Unlaunched Campaign Launch for campaignId " + play.getName() + ", Channel ID: "
                    + channel.getId() + " Launch Id: " + launch.getLaunchId());

            url = constructUrl(campaignDeltaCalculationUrlPrefix + launch.getLaunchId(),
                    CustomerSpace.parse(channel.getTenant().getId()).getTenantId(), channel.getPlay().getName(),
                    channel.getId());
            Long workflowPid = post("Kicking off delta calculation", url, null, Long.class);
            log.info("Queued a delta calculation job for campaignId " + channel.getPlay().getName() + ", Channel ID: "
                    + channel.getId() + " Launch Id: " + launch.getLaunchId() + "  WorkflowPid: " + workflowPid);
            return true;
        } catch (Exception e) {
            log.error("Failed to Kick off delta calculation for channel: " + channel.getId() + " \n", e);
            return false;
        }
    }

    private boolean queueNewFullCampaignLaunch(PlayLaunchChannel channel) {
        try {
            Play play = channel.getPlay();
            String url = constructUrl(campaignLaunchUrlPrefix + LaunchState.Queued.name(),
                    CustomerSpace.parse(channel.getTenant().getId()).getTenantId(), play.getName(), channel.getId());

            PlayLaunch launch = post("Kicking off Campaign Launch", url, null, PlayLaunch.class);
            log.info("Queued a Campaign Launch for campaignId " + play.getName() + ", Channel ID: " + channel.getId()
                    + " Launch Id: " + launch.getLaunchId() + "  ApplicationId: " + launch.getApplicationId());

            url = constructUrl(setChannelScheduleUrlPrefix,
                    CustomerSpace.parse(channel.getTenant().getId()).getTenantId(), play.getName(), channel.getId());
            log.info("Setting Next Scheduled Date for campaignId " + play.getName() + ", Channel ID: "
                    + channel.getId());
            channel = patch("Setting Next Scheduled Date", url, null, PlayLaunchChannel.class);
            return true;
        } catch (Exception e) {
            log.error("Failed to Kick off Campaign Launch for channel: " + channel.getId() + " \n", e);
            return false;
        }
    }
}
