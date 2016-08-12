package com.latticeengines.propdata.engine.ingestion.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;

public interface IngestionService {
    public Ingestion getIngestionByName(String ingestionName);

    public IngestionProgress ingestInternal(String ingestionName, IngestionRequest ingestionRequest,
            String hdfsPod);

    public List<IngestionProgress> scan(String hdfsPod);

    public void checkCompleteIngestions();

    public List<String> getMissingFiles(Ingestion ingestion);

    public List<String> getTargetFiles(Ingestion ingestion);

    public List<String> getExistingFiles(Ingestion ingestion);

    public void killFailedProgresses();
}
