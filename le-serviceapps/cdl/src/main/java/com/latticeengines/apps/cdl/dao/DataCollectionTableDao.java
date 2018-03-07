package com.latticeengines.apps.cdl.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;

public interface DataCollectionTableDao extends BaseDao<DataCollectionTable> {

    List<DataCollectionTable> findAllByName(String collectionName, String tableName, DataCollection.Version version);

}
