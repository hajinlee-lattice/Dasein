package com.latticeengines.apps.cdl.service;

import org.apache.commons.lang3.tuple.Pair;

public interface S3ImportFolderService {

    void initialize(String tenantId);

    String getBucket();

    Pair<String, String> startImport(String tenantId, String entity, String sourceBucket, String sourceKey);

    String moveFromInProgressToSucceed(String key);

    String moveFromInProgressToFailed(String key);

}
