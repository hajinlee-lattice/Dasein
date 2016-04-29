package com.latticeengines.propdata.engine.ingestion.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;

public interface IngestionNewProgressValidator {
    public boolean isIngestionTriggered(Ingestion ingestion);

    public boolean isDuplicateProgress(IngestionProgress progress);

    public List<IngestionProgress> checkDuplcateProgresses(List<IngestionProgress> progresses);
}
