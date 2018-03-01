package com.latticeengines.datacloudapi.engine.purge.service.impl;

import org.springframework.stereotype.Component;

@Component("pipelineTempSourcePurger")
public class PipelineTempSourcePurger extends PatternedPurger {

    public String getSourcePrefix() {
        return "Pipeline_";
    }

    public int getRetainDays() {
        return 7;
    }

    public boolean isToBak() {
        return false;
    }

}
