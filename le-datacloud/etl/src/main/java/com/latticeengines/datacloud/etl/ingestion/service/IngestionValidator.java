package com.latticeengines.datacloud.etl.ingestion.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;

public interface IngestionValidator {
    boolean isIngestionTriggered(Ingestion ingestion);

    void validateIngestionRequest(Ingestion ingestion, IngestionRequest request);

    boolean isDuplicateProgress(IngestionProgress progress);

    List<IngestionProgress> cleanupDuplicateProgresses(List<IngestionProgress> progresses);
}
