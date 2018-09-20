package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.service.CollectionDBService;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;

@Component("ingestionBWRawProviderService")
public class IngestionBWRawProviderServiceImpl extends IngestionProviderServiceImpl {
    private static final Logger log = LoggerFactory.getLogger(IngestionBWRawProviderServiceImpl.class);

    @Inject
    private CollectionDBService collectionDBService;

    @Override
    public void ingest(IngestionProgress progress) {
        collectionDBService.ingest();
    }

    @Override
    public List<String> getMissingFiles(Ingestion ingestion) {
        return Collections.emptyList();
    }
}
