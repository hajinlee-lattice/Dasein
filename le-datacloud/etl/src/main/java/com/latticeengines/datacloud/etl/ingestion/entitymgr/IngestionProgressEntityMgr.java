package com.latticeengines.datacloud.etl.ingestion.entitymgr;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;

public interface IngestionProgressEntityMgr {

    IngestionProgress findProgress(IngestionProgress progress);

    List<IngestionProgress> findProgressesByField(Map<String, Object> fields, List<String> orderFields);

    IngestionProgress saveProgress(IngestionProgress progress);

    void deleteProgress(IngestionProgress progress);

    void deleteProgressByField(Map<String, Object> fields);

    List<IngestionProgress> findRetryFailedProgresses();

    boolean isDuplicateProgress(IngestionProgress progress);
}
