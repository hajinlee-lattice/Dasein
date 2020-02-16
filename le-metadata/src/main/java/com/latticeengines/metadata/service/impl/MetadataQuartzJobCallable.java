package com.latticeengines.metadata.service.impl;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.serviceapps.cdl.MetadataJobType;
import com.latticeengines.metadata.service.MetadataMigrateDynamoService;
import com.latticeengines.metadata.service.MetadataTableCleanupService;

public class MetadataQuartzJobCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(MetadataQuartzJobCallable.class);

    private String jobArguments;
    private MetadataJobType metadataJobType;
    private MetadataTableCleanupService metadataTableCleanupService;
    private MetadataMigrateDynamoService metadataMigrateDynamoService;

    public MetadataQuartzJobCallable(Builder builder) {
        this.jobArguments = builder.jobArguments;
        this.metadataJobType = builder.metadataJobType;
        this.metadataTableCleanupService = builder.metadataTableCleanupService;
        this.metadataMigrateDynamoService = builder.metadataMigrateDynamoService;
    }

    @Override
    public Boolean call() {
        log.info(String.format("Calling with job type: %s", metadataJobType.name()));
        switch (metadataJobType) {
            case TABLE_CLEANUP:
                return metadataTableCleanupService.cleanup();
            case MIGRATE_DYNAMO:
                return metadataMigrateDynamoService.migrateDynamo();
            default:
                return true;
        }
    }

    public static class Builder {
        private String jobArguments;
        private MetadataJobType metadataJobType;
        private MetadataTableCleanupService metadataTableCleanupService;
        private MetadataMigrateDynamoService metadataMigrateDynamoService;

        public Builder() {

        }

        public Builder metadataJobType(MetadataJobType metadataJobType) {
            this.metadataJobType = metadataJobType;
            return this;
        }

        public Builder jobArguments(String jobArguments) {
            this.jobArguments = jobArguments;
            return this;
        }

        public Builder metadataTableCleanupService(MetadataTableCleanupService metadataTableCleanupService) {
            this.metadataTableCleanupService = metadataTableCleanupService;
            return this;
        }

        public Builder metadataMigrateDynamoService(MetadataMigrateDynamoService metadataMigrateDynamoService) {
            this.metadataMigrateDynamoService = metadataMigrateDynamoService;
            return this;
        }
    }
}
