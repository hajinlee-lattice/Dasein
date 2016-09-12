package com.latticeengines.propdata.engine.ingestion.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;

public interface IngestionNewProgressValidator {
    public boolean isIngestionTriggered(Ingestion ingestion);

    public boolean isDuplicateProgress(IngestionProgress progress);

    public List<IngestionProgress> checkDuplicateProgresses(List<IngestionProgress> progresses);
}
