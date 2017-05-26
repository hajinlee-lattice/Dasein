package com.latticeengines.datacloud.etl.ingestion.entitymgr;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;

public interface IngestionProgressEntityMgr {

    IngestionProgress getProgress(IngestionProgress progress);

    List<IngestionProgress> getProgressesByField(Map<String, Object> fields, List<String> orderFields);

    IngestionProgress saveProgress(IngestionProgress progress);

    void deleteProgress(IngestionProgress progress);

    void deleteProgressByField(Map<String, Object> fields);

    boolean isIngestionTriggered(Ingestion ingestion);

    List<IngestionProgress> getRetryFailedProgresses();

    boolean isDuplicateProgress(IngestionProgress progress);
}
