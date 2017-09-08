package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface RatingEngineDao extends BaseDao<RatingEngine> {

    RatingEngine findById(String id);

}
