package com.latticeengines.metadata.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public interface DataCollectionDao extends BaseDao<DataCollection> {

    List<String> getTableNamesOfRole(String collectionName, TableRoleInCollection tableRole,
            DataCollection.Version version);

}
