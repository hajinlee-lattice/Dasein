package com.latticeengines.datacloud.etl.ingestion.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;

public interface IngestionProviderService {

    void ingest(IngestionProgress progress) throws Exception;

    /**
     * Find files to ingest
     *
     * @param ingestion
     * @return file list to ingest
     */
    List<String> getMissingFiles(Ingestion ingestion);
}
