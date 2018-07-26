package com.latticeengines.apps.cdl.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;

public interface AIModelDao extends BaseDao<AIModel> {

    MetadataSegment findParentSegmentById(String id);

    void create(AIModel aiModel);

    int findMaxIterationByRatingEngineId(String ratineEngindId);

}
