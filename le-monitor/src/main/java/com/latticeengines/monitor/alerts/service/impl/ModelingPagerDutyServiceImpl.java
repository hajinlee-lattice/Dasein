package com.latticeengines.monitor.alerts.service.impl;

import org.springframework.stereotype.Component;

@Component("modelingPagerDutyService")
public class ModelingPagerDutyServiceImpl extends BasePagerDutyServiceImpl {

    private static final String MODELINGPLATFORM_SERVICEAPI_KEY = "62368b3c576e4f6180dba752216fd487";
    private static final String TEST_SERVICEAPI_KEY = "c6ca7f8f643c4db4a475bae9a504552d";

    public ModelingPagerDutyServiceImpl() {
        serviceApiKey = MODELINGPLATFORM_SERVICEAPI_KEY;
    }

    @Override
    protected String getToken() {
        return "VjqbZdWQbwq2Fy7gniny";
    }

    @Override
    protected String getModuleName() {
        return "Modeling Platform";
    }

    @Override
    protected String getTestServiceApiKey() {
        return TEST_SERVICEAPI_KEY;
    }

}
