package com.latticeengines.metadata.service.impl;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.metadata.service.MetadataTableCleanupService;

public class MetadataTableCleanupJobCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(MetadataTableCleanupJobCallable.class);

    private String jobArguments;
    private MetadataTableCleanupService metadataTableCleanupService;

    public MetadataTableCleanupJobCallable(Builder builder) {
        this.jobArguments = builder.jobArguments;
        this.metadataTableCleanupService = builder.metadataTableCleanupService;
    }

    @Override
    public Boolean call() {
        metadataTableCleanupService.cleanup();
        return true;
    }

    public static class Builder {
        private String jobArguments;
        private MetadataTableCleanupService metadataTableCleanupService;

        public Builder() {

        }

        public Builder jobArguments(String jobArguments) {
            this.jobArguments = jobArguments;
            return this;
        }

        public Builder metadataTableCleanupService(MetadataTableCleanupService metadataTableCleanupService) {
            this.metadataTableCleanupService = metadataTableCleanupService;
            return this;
        }

    }
}
