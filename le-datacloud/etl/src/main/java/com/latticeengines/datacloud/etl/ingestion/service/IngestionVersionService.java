package com.latticeengines.datacloud.etl.ingestion.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.ingestion.FileCheckStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.orchestration.EngineProgress;

public interface IngestionVersionService {
    List<String> getMostRecentVersionsFromHdfs(String ingestionName, int checkVersion);

    String extractVersion(String timestampFormat, String str);

    String getFileNamePattern(String version, String fileNamePrefix, String fileNamePostfix, String fileExtension,
            String fileTimestamp);

    List<String> getFileNamesOfMostRecentVersions(List<String> fileNames, int checkVersion,
            FileCheckStrategy checkStrategy, String fileTimestamp);

    void updateCurrentVersion(Ingestion ingestion, String version);

    boolean isCompleteVersion(Ingestion ingestion, String version);

    String getCurrentVersion(Ingestion ingestion);

    EngineProgress status(String ingestionName, String version);
}
