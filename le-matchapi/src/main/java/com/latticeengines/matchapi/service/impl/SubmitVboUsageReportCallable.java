package com.latticeengines.matchapi.service.impl;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datacloud.match.service.VboUsageService;

public class SubmitVboUsageReportCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(SubmitVboUsageReportCallable.class);

    private VboUsageService vboUsageService;

    public SubmitVboUsageReportCallable(Builder builder) {
        this.vboUsageService = builder.getVboUsageService();
    }

    @Override
    public Boolean call() {
        log.info("SubmitVboUsageReport is triggered!");
        vboUsageService.moveReportsToVbo();
        return true;
    }

    public static class Builder {
        private VboUsageService vboUsageService;

        public Builder() {
        }

        public VboUsageService getVboUsageService() {
            return vboUsageService;
        }

        public Builder vboUsageService(VboUsageService vboUsageService) {
            this.vboUsageService = vboUsageService;
            return this;
        }

    }

}
