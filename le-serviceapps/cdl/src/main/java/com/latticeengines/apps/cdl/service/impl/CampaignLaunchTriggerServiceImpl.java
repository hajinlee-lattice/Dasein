package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.CampaignLaunchTriggerService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.workflow.PlayLaunchWorkflowSubmitter;
import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@Component("campaignLaunchTriggerService")
public class CampaignLaunchTriggerServiceImpl implements CampaignLaunchTriggerService {
    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchTriggerServiceImpl.class);

    @Value("${cdl.campaignLaunch.maximum.job.count}")
    private Long maxToLaunch;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchWorkflowSubmitter playLaunchWorkflowSubmitter;

    @Override
    public Boolean triggerQueuedLaunches() {
        List<PlayLaunch> queuedPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Queued,
                maxToLaunch);
        List<PlayLaunch> launchingPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Launching,
                maxToLaunch);
        for (int i = launchingPlayLaunches.size(); i < maxToLaunch; i++) {
            if (queuedPlayLaunches.isEmpty()) {
                log.info("No more Queued Play Launches available.");
                return true;
            }
            PlayLaunch playLaunch = queuedPlayLaunches.get(0);
            HibernateUtils.inflateDetails(playLaunch.getPlay());

            String appId = playLaunchWorkflowSubmitter.submit(playLaunch).toString();
            playLaunch.setApplicationId(appId);

            playLaunch.setLaunchState(LaunchState.Launching);
            playLaunchService.update(playLaunch);
        }
        return true;
    }

}
