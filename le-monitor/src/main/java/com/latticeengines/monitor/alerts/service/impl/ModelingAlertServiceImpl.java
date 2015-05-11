package com.latticeengines.monitor.alerts.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("modelingAlertService")
public class ModelingAlertServiceImpl extends BaseAlertServiceImpl {

    @Autowired
    public ModelingAlertServiceImpl(@Qualifier("modelingPagerDutyService") BasePagerDutyServiceImpl modelingPagerDutyService) {
        pagerDutyService = modelingPagerDutyService;
    }
}
