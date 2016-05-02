package com.latticeengines.propdata.engine.ingestion.entitymgr;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;

public interface IngestionProgressEntityMgr {

    public IngestionProgress getProgress(IngestionProgress progress);

    public List<IngestionProgress> getProgressesByField(Map<String, Object> fields);

    public IngestionProgress saveProgress(IngestionProgress progress);

    public void deleteProgress(IngestionProgress progress);

    public void deleteProgressByField(Map<String, Object> fields);

    public boolean isIngestionTriggered(Ingestion ingestion);

    public List<IngestionProgress> getRetryFailedProgresses();

    public boolean isDuplicateProgress(IngestionProgress progress);
}
