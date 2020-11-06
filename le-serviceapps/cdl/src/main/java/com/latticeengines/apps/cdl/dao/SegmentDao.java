package com.latticeengines.apps.cdl.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;

public interface SegmentDao extends BaseDao<MetadataSegment> {

    MetadataSegment findMasterSegment(String collectionName);

    List<String> getAllDeletedSegments();
}
