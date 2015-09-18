package com.latticeengines.propdata.api.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.DataColumnMap;

public interface DataColumnMapDao extends BaseDao<DataColumnMap> {

    DataColumnMap findByContent(String extensionName, String sourceTableName);

    List<DataColumnMap> findByColumnCalcIds(List<Long> ids);

}
