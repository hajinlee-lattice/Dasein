package com.latticeengines.datacloud.etl.ingestion.service;

import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

public interface IngestionVersionService {
    void updateCurrentVersion(Ingestion ingestion, String version);

    boolean isCompleteVersion(Ingestion ingestion, String version);

    String findCurrentVersion(Ingestion ingestion);

    ProgressStatus findProgressAtVersion(String ingestionName, String version);
}
