package com.latticeengines.modelquality.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.DataSet;

public interface DataSetEntityMgr extends BaseEntityMgr<DataSet> {

    DataSet findByName(String name);
    
    DataSet findByTenantAndTrainingSet(String tenantID, String trainingSetFilePath);
}
