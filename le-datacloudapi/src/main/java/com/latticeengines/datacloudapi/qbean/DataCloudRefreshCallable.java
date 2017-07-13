package com.latticeengines.datacloudapi.qbean;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datacloudapi.engine.ingestion.service.IngestionService;
import com.latticeengines.datacloudapi.engine.publication.service.PublicationService;
import com.latticeengines.datacloudapi.engine.transformation.service.SourceTransformationService;


public class DataCloudRefreshCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(DataCloudRefreshCallable.class);

    private final SourceTransformationService transformationService;
    private final PublicationService publicationService;
    private final IngestionService ingestionService;

    private DataCloudRefreshCallable(SourceTransformationService transformationProxy, PublicationService publicationProxy,
                                     IngestionService ingestionProxy) {

        this.transformationService = transformationProxy;
        this.publicationService = publicationProxy;
        this.ingestionService = ingestionProxy;
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

        return true;
    }

    public static class Builder {

        private SourceTransformationService transformationService;
        private PublicationService publicationService;
        private IngestionService ingestionService;

        public DataCloudRefreshCallable build() {
            return new DataCloudRefreshCallable(transformationService, publicationService, ingestionService);
        }

        Builder transformationProxy(SourceTransformationService transformationProxy) {
            this.transformationService = transformationProxy;
            return this;
        }

        Builder publicationProxy(PublicationService publicationProxy) {
            this.publicationService = publicationProxy;
            return this;
        }

        Builder ingestionProxy(IngestionService ingestionProxy) {
            this.ingestionService = ingestionProxy;
            return this;
        }

    }

}
