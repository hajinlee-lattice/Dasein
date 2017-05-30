package com.latticeengines.datacloud.etl.ingestion.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;

public interface IngestionProviderService {
    List<String> getMissingFiles(Ingestion ingestion);
}
