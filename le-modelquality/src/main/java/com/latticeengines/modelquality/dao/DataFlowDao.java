package com.latticeengines.modelquality.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.modelquality.DataFlow;

public interface DataFlowDao extends BaseDao<DataFlow> {
    DataFlow findByMaxVersion();
}
