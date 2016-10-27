package com.latticeengines.propdata.engine.ingestion.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;

public interface IngestionService {
    Ingestion getIngestionByName(String ingestionName);

    IngestionProgress ingestInternal(String ingestionName, IngestionRequest ingestionRequest,
            String hdfsPod);

    List<IngestionProgress> scan(String hdfsPod);

    void checkCompleteIngestions();

    List<String> getMissingFiles(Ingestion ingestion);

    List<String> getTargetFiles(Ingestion ingestion);

    List<String> getExistingFiles(Ingestion ingestion);

    void killFailedProgresses();
}
