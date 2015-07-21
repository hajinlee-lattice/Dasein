package com.latticeengines.propdata.api.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.DataColumnMap;

public interface DataColumnMapDao extends BaseDao<DataColumnMap> {

    DataColumnMap findByContent(String extensionName, String sourceTableName);

}
