package com.latticeengines.metadata.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

import java.util.List;

public interface DataCollectionDao extends BaseDao<DataCollection> {

    List<String> getTableNamesOfRole(String collectionName, TableRoleInCollection tableRole);

}
