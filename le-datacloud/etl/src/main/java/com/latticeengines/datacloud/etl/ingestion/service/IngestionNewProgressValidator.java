package com.latticeengines.datacloud.etl.ingestion.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;

public interface IngestionNewProgressValidator {
    boolean isIngestionTriggered(Ingestion ingestion);

    boolean isDuplicateProgress(IngestionProgress progress);

    List<IngestionProgress> checkDuplicateProgresses(List<IngestionProgress> progresses);
}
