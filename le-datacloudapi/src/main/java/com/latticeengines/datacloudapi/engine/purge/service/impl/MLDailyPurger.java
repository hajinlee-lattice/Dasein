package com.latticeengines.datacloudapi.engine.purge.service.impl;

import org.springframework.stereotype.Component;

@Component("mlDailyPurger")
public class MLDailyPurger extends MLSourcePurger {

    @Override
    protected String getRootPath() {
        return "/user/propdata/madison/dataflow/incremental";
    }

}
