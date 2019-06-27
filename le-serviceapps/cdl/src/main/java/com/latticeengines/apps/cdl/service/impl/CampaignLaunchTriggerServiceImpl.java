package com.latticeengines.apps.cdl.service.impl;

import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.CampaignLaunchTriggerService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("campaignLaunchTriggerService")
public class CampaignLaunchTriggerServiceImpl extends BaseRestApiProxy implements CampaignLaunchTriggerService {
    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchTriggerServiceImpl.class);

    @Value("${cdl.campaignLaunch.maximum.job.count}")
    private Long maxToLaunch;

    @Value("common.internal.app.url")
    private String internalAppUrl;

    @Inject
    private PlayLaunchService playLaunchService;

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
        if (launchingPlayLaunches.size() > maxToLaunch) {
            log.info(String.format("%s Launch jobs are currently running, no new jobs can be kicked off ",
                    launchingPlayLaunches.size()));
            return true;
        }

        Iterator<PlayLaunch> iterator = queuedPlayLaunches.iterator();
        int i = launchingPlayLaunches.size();

        while (i < maxToLaunch && iterator.hasNext()) {
            PlayLaunch launch = iterator.next();
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

}
