package com.latticeengines.metadata.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;

public interface DataCollectionTableDao extends BaseDao<DataCollectionTable> {

    DataCollectionTable findByNames(String collectionName, String tableName);

}
