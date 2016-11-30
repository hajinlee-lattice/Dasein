package com.latticeengines.modelquality.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.modelquality.PropData;

public interface PropDataDao extends BaseDao<PropData> {
    PropData findByMaxVersion();
}
