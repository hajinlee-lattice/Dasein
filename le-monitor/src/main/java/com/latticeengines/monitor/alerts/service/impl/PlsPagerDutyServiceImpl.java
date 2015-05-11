package com.latticeengines.monitor.alerts.service.impl;

import org.springframework.stereotype.Component;

@Component("plsPagerDutyService")
public class PlsPagerDutyServiceImpl extends BasePagerDutyServiceImpl {

    private static final String PLS_SERVICEAPI_KEY = "d3eb9c2d98b34f12a5a4915525a2e3ed";
    private static final String TEST_SERVICEAPI_KEY = "e0ac6d90e42442cf91a814b162a84796";

    public PlsPagerDutyServiceImpl() {
        serviceApiKey = PLS_SERVICEAPI_KEY;
    }

    @Override
    protected String getToken() {
        return "UxFR6wR3EjVM1yRK6yZR";
    }

    @Override
    protected String getModuleName() {
        return "PLS Multi-tenant";
    }

    @Override
    protected String getTestServiceApiKey() {
        return TEST_SERVICEAPI_KEY;
    }

}
