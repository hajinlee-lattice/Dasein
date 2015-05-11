package com.latticeengines.monitor.alerts.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("plsAlertService")
public class PlsAlertServiceImpl extends BaseAlertServiceImpl {

    @Autowired
    public PlsAlertServiceImpl(@Qualifier("plsPagerDutyService") BasePagerDutyServiceImpl plsPagerDutyService) {
        pagerDutyService = plsPagerDutyService;
    }
}
