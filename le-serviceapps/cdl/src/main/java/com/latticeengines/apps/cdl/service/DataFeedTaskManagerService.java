package com.latticeengines.apps.cdl.service;

public interface DataFeedTaskManagerService {

    String createDataFeedTask(String feedType, String entity, String source, String metadata);

    String submitImportJob(String taskIdentifier, String importConfig);
}
