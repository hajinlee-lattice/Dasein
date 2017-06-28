package com.latticeengines.pls.service;

public interface DataFeedTaskManagerService {

    String createDataFeedTask(String feedType, String entity, String source, String datafeedName, String metadata);

    String submitImportJob(String taskIdentifier, String source, String importConfig);
}
