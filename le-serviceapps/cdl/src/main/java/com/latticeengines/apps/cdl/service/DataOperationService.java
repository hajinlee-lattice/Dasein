package com.latticeengines.apps.cdl.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.cdl.DataOperationConfiguration;
import com.latticeengines.domain.exposed.cdl.DataOperationRequest;
import com.latticeengines.domain.exposed.metadata.DataOperation;

public interface DataOperationService {
    String createDataOperation(String customerSpace, DataOperation.OperationType operationType,
                               DataOperationConfiguration configuration);

    void updateDataOperation(String customerSpace, DataOperation dataOperation);

    List<DataOperation> findAllDataOperation(String customerSpace);

    DataOperation findDataOperationByDropPath(String customerSpace, String dropPath);

    void deleteDataOperation(String customerSpace, DataOperation dataOperation);

    ApplicationId submitJob(String customerSpace, DataOperationRequest dataOperationRequest);
}
