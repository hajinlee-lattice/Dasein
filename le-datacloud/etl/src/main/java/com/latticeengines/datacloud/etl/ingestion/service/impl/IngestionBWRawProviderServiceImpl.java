package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.service.CollectionDBService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

@Component("ingestionBWRawProviderService")
public class IngestionBWRawProviderServiceImpl extends IngestionProviderServiceImpl {
    private static final Logger log = LoggerFactory.getLogger(IngestionBWRawProviderServiceImpl.class);

    @Inject
    private IngestionProgressService ingestionProgressService;

    @Inject
    private CollectionDBService collectionDBService;

    @Override
    public void ingest(IngestionProgress progress) {
        collectionDBService.ingest();
        progress = ingestionProgressService.updateProgress(progress).status(ProgressStatus.FINISHED).commit(true);
    }

    @Override
    public List<String> getMissingFiles(Ingestion ingestion) {
        int tasksToIngest = collectionDBService.getIngestionTaskCount();
        return tasksToIngest == 0 ? Collections.emptyList() : Arrays.asList(UUID.randomUUID().toString());
    }
}
