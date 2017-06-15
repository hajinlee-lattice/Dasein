package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;

public interface SegmentEntityMgr extends BaseEntityMgr<MetadataSegment> {

    MetadataSegment findByName(String name);

    MetadataSegment findByName(String dataSourceName, String name);

    List<MetadataSegment> findAllInCollection(String collectionName);

    MetadataSegment findMasterSegment(String collectionName);
}
