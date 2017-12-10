package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.CDLImportConfig;

public interface DataFeedTaskManagerService {

    String createDataFeedTask(String customerSpaceStr, String feedType, String entity, String source,
                              CDLImportConfig importConfig);

    String submitImportJob(String customerSpaceStr, String taskIdentifier, CDLImportConfig importConfig);
}
