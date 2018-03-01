package com.latticeengines.datacloudapi.engine.purge.service.impl;

import org.springframework.stereotype.Component;

@Component("operationalSourcePurger")
public class OperationalSourcePurger extends PatternedSourcePurger {
    public String getSourcePrefix() {
        return "LDCDEV_";
    }

    public int getRetainDays() {
        return 14;
    }

    public boolean isToBak() {
        return false;
    }
}
