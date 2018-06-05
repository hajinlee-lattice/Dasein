package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface DataFeedTaskManagerService {

    String createDataFeedTask(String customerSpaceStr, String feedType, String entity, String source,
                              CDLImportConfig importConfig);

    String submitImportJob(String customerSpaceStr, String taskIdentifier, CDLImportConfig importConfig);

    boolean resetImport(String customerSpaceStr, BusinessEntity entity);
}
