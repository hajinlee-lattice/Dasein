package com.latticeengines.datacloudapi.engine.purge.service.impl;

import org.springframework.stereotype.Component;

@Component("ml30DayPurger")
public class ML30DaysPurger extends MLSourcePurger {

    @Override
    protected String getRootPath() {
        return "/user/propdata/madison/workflow/MadisonLogic-Days-30/total_aggregation";
    }

}
