package com.latticeengines.apps.cdl.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.CampaignLaunchTriggerService;

@Component("campaignLaunchTriggerService")
public class CampaignLaunchTriggerServiceImpl implements CampaignLaunchTriggerService {
    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchTriggerServiceImpl.class);

    public Boolean triggerQueuedLaunches() {
        return null;
    }

}
