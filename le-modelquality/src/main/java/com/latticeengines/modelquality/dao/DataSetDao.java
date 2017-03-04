package com.latticeengines.modelquality.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.modelquality.DataSet;

public interface DataSetDao extends BaseDao<DataSet> {

    DataSet findByTenantAndTrainingSet(String tenantID, String trainingSetFilePath);

}
