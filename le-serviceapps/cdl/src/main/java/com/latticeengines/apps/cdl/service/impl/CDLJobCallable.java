package com.latticeengines.apps.cdl.service.impl;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public class CDLJobCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(CDLJobCallable.class);

    private CDLJobType cdlJobType;
    private CDLJobService cdlJobService;

    public CDLJobCallable(Builder builder) {
        this.cdlJobType = builder.cdlJobType;
        this.cdlJobService = builder.cdlJobService;
    }

    @Override
    public Boolean call() throws Exception {
        log.info(String.format("Calling with jobtype: %s", cdlJobType.name()));
        return null;
    }

    public static class Builder {

        private CDLJobType cdlJobType;
        private CDLJobService cdlJobService;

        public Builder() {

        }

        public Builder cdlJobType(CDLJobType cdlJobType) {
            this.cdlJobType = cdlJobType;
            return this;
        }

        public Builder cdlJobService(CDLJobService cdlJobService) {
            this.cdlJobService = cdlJobService;
            return this;
        }
    }
}
