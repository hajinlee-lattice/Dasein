package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.CampaignLaunchTriggerService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.workflow.PlayLaunchWorkflowSubmitter;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.util.PlayUtils;

@Component("campaignLaunchTriggerService")
public class CampaignLaunchTriggerServiceImpl implements CampaignLaunchTriggerService {
    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchTriggerServiceImpl.class);

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchWorkflowSubmitter playLaunchWorkflowSubmitter;

    @Override
    public Boolean triggerQueuedLaunches() {
        Boolean isLaunchedOnce = false;
        List<PlayLaunch> queuedPlayLaunches = playLaunchService.findByState(LaunchState.Queued);
        List<PlayLaunch> launchingPlayLaunches = playLaunchService.findByState(LaunchState.Launching);
        for (int i = launchingPlayLaunches.size(); i < 10; i++) {
            if (queuedPlayLaunches.isEmpty()) {
                return isLaunchedOnce;
            }
            PlayLaunch playLaunch = queuedPlayLaunches.get(0);

            // TODO: ask if I need to do these validations. If I do can I just call
            // playLaunch.getPlay() even if its eager fetch lazy?
            
            Play play = playLaunch.getPlay();
            PlayUtils.validatePlay(play.getName(), play);
            PlayUtils.validatePlayLaunchBeforeLaunch(playLaunch, play);

            String appId = playLaunchWorkflowSubmitter.submit(playLaunch).toString();
            playLaunch.setApplicationId(appId);

            playLaunch.setLaunchState(LaunchState.Launching);
            playLaunch.setPlay(play);

            // TODO: ask if I need to implement this method
            // playLaunch.setTableName(createTable(playLaunch));

            Long totalAvailableRatedAccounts = play.getTargetSegment().getAccounts();
            Long totalAvailableContacts = play.getTargetSegment().getContacts();

            playLaunch.setAccountsSelected(totalAvailableRatedAccounts != null ? totalAvailableRatedAccounts : 0L);
            playLaunch.setAccountsSuppressed(0L);
            playLaunch.setAccountsErrored(0L);
            playLaunch.setAccountsLaunched(0L);
            playLaunch.setContactsSelected(totalAvailableContacts != null ? totalAvailableContacts : 0L);
            playLaunch.setContactsLaunched(0L);
            playLaunch.setContactsSuppressed(0L);
            playLaunch.setContactsErrored(0L);
            playLaunchService.update(playLaunch);
            isLaunchedOnce = true;
        }
        return isLaunchedOnce;
    }

}
