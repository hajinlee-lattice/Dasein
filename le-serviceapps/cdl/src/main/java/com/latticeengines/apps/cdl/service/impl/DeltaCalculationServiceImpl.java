package com.latticeengines.apps.cdl.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DeltaCalculationService;

@Component("deltaCalculationService")
public class DeltaCalculationServiceImpl implements DeltaCalculationService {
    private static final Logger log = LoggerFactory.getLogger(DeltaCalculationServiceImpl.class);

    public Boolean triggerScheduledCampaigns() {
        return null;
    }
}
