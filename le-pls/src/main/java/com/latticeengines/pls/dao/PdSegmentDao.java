package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.Segment;

public interface PdSegmentDao extends BaseDao<Segment> {

    Segment findByName(String name);

    Segment findByModelId(String modelId);
}
