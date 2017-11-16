package com.latticeengines.pls.service.impl;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.pls.service.MetadataSegmentExportCleanupService;

public class MetadataSegmentExportCleanupCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(MetadataSegmentExportCleanupCallable.class);

    private MetadataSegmentExportCleanupService metadataSegmentExportCleanupService;

    public MetadataSegmentExportCleanupCallable(Builder builder) {
        this.metadataSegmentExportCleanupService = builder.getMetadataSegmentExportCleanupService();
    }

    @Override
    public Boolean call() throws Exception {
        log.info("Received call for cleanup");
        metadataSegmentExportCleanupService.cleanup();
        return true;
    }

    public static class Builder {
        private MetadataSegmentExportCleanupService metadataSegmentExportCleanupService;

        public Builder() {
        }

        public MetadataSegmentExportCleanupService getMetadataSegmentExportCleanupService() {
            return metadataSegmentExportCleanupService;
        }

        public Builder metadataSegmentExportCleanupService(
                MetadataSegmentExportCleanupService metadataSegmentExportCleanupService) {
            this.metadataSegmentExportCleanupService = metadataSegmentExportCleanupService;
            return this;
        }
    }

}
