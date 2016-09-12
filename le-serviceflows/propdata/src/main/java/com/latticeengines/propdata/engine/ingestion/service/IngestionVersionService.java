package com.latticeengines.propdata.engine.ingestion.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.ingestion.FileCheckStrategy;

public interface IngestionVersionService {
    public List<String> getMostRecentVersionsFromHdfs(String ingestionName, int checkVersion);

    public String getFileNamePattern(String version, String fileNamePrefix, String fileNamePostfix,
            String fileExtension, String fileTimestamp);

    public List<String> getFileNamesOfMostRecentVersions(List<String> fileNames, int checkVersion,
            FileCheckStrategy checkStrategy, String fileTimestamp);
}
