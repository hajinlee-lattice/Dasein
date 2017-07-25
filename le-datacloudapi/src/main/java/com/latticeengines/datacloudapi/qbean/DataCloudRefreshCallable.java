package com.latticeengines.datacloudapi.qbean;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datacloudapi.engine.ingestion.service.IngestionService;
import com.latticeengines.datacloudapi.engine.orchestration.service.OrchestrationService;
import com.latticeengines.datacloudapi.engine.publication.service.PublicationService;
import com.latticeengines.datacloudapi.engine.transformation.service.SourceTransformationService;


public class DataCloudRefreshCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(DataCloudRefreshCallable.class);

    private final SourceTransformationService transformationService;
    private final PublicationService publicationService;
    private final IngestionService ingestionService;
    private final OrchestrationService orchestrationService;

    private DataCloudRefreshCallable(SourceTransformationService transformationService,
            PublicationService publicationService, IngestionService ingestionService,
            OrchestrationService orchestrationService) {
        this.transformationService = transformationService;
        this.publicationService = publicationService;
        this.ingestionService = ingestionService;
        this.orchestrationService = orchestrationService;
    }

    @Override
    public Boolean call() {

        try {
            publicationService.scan("");
        } catch (Exception e) {
            log.error("Failed to scan publication engine", e);
        }

        try {
            transformationService.scan("");
        } catch (Exception e) {
            log.error("Failed to scan transformation engine", e);
        }

        try {
            ingestionService.scan("");
        } catch (Exception e) {
            log.error("Failed to scan ingestion engine", e);
        }

        try {
            orchestrationService.scan("");
        } catch (Exception e) {
            log.error("Failed to scan orchestration engine", e);
        }

        return true;
    }

    public static class Builder {

        private SourceTransformationService transformationService;
        private PublicationService publicationService;
        private IngestionService ingestionService;
        private OrchestrationService orchestrationService;

        public DataCloudRefreshCallable build() {
            return new DataCloudRefreshCallable(transformationService, publicationService, ingestionService,
                    orchestrationService);
        }

        Builder transformationService(SourceTransformationService transformationService) {
            this.transformationService = transformationService;
            return this;
        }

        Builder publicationService(PublicationService publicationService) {
            this.publicationService = publicationService;
            return this;
        }

        Builder ingestionService(IngestionService ingestionService) {
            this.ingestionService = ingestionService;
            return this;
        }

        Builder orchestrationService(OrchestrationService orchestrationService) {
            this.orchestrationService = orchestrationService;
            return this;
        }

    }

}
