package com.latticeengines.serviceflows.workflow.service;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.PrestoDataUnit;

public interface WorkflowDataUnitService {
    
    PrestoDataUnit registerPrestoDataUnit(HdfsDataUnit hdfsDataUnit);
    
}
