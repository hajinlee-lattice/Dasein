package com.latticeengines.apps.cdl.service;

public interface S3ImportFolderService {

    void initialize(String tenantId);

    String getBucket();

    String startImport(String tenantId, String entity, String sourceBucket, String sourceKey, boolean state);

    String moveFromInProgressToSucceed(String key);

    String moveFromInProgressToFailed(String key);

}
