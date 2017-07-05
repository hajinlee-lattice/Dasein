package com.latticeengines.apps.cdl.service;

public interface DataFeedTaskManagerService {

    String createDataFeedTask(String customerSpaceStr, String feedType, String entity, String source, String metadata);

    String submitImportJob(String customerSpaceStr, String taskIdentifier, String importConfig);
}
