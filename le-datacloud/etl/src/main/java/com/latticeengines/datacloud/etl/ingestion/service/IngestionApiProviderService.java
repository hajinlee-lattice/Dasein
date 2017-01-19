package com.latticeengines.datacloud.etl.ingestion.service;

import com.latticeengines.domain.exposed.datacloud.ingestion.ApiConfiguration;

public interface IngestionApiProviderService {
    String getTargetVersion(ApiConfiguration config);
}
