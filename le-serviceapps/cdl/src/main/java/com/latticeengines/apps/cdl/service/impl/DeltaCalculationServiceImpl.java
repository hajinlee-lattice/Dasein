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

    private final String campaignDeltaCalculationUrlPrefix = "/customerspaces/{customerSpace}/plays/{playId}/channels/{channelId}/kickoff-delta-calculation";

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

        channels = channels.stream().filter(c -> batonService.isEnabled(CustomerSpace.parse(c.getTenant().getId()),
                LatticeFeatureFlag.ALWAYS_ON_CAMPAIGNS)).collect(Collectors.toList());

        log.info("Found " + channels.size() + " channels scheduled for launch");

        long successfullyQueued = channels.stream().map(this::queueNewDeltaCalculationJob).filter(x -> x).count();

        log.info(
                String.format("Total Delta Calculation Jobs to be Scheduled: %s, Queued: %s, Job Submission failed: %s",
                        channels.size(), //
                        successfullyQueued, //
                        channels.size() - successfullyQueued));
        return true;
    }

    private boolean queueNewDeltaCalculationJob(PlayLaunchChannel channel) {
        try {
            String url = constructUrl(campaignDeltaCalculationUrlPrefix,
                    CustomerSpace.parse(channel.getTenant().getId()).getTenantId(), channel.getPlay().getName(),
                    channel.getId());
            Long workflowPid = post("Kicking off delta calculation", url, null, Long.class);
            log.info("Queued a delta calculation job for campaignId " + channel.getPlay().getName() + ", Channel ID: "
                    + channel.getId() + "  WorkflowPid: " + workflowPid);
            return true;
        } catch (Exception e) {
            log.error("Failed to Kick off delta calculation\n", e);
            return false;
        }
    }
}
