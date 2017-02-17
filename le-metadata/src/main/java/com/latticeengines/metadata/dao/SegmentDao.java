package com.latticeengines.metadata.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;

public interface SegmentDao extends BaseDao<MetadataSegment> {

    MetadataSegment findByDataCollectionAndName(String querySourceName, String name);

    MetadataSegment findByNameWithDefaultDataCollection(String name);
}
