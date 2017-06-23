package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;

public interface SegmentEntityMgr extends BaseEntityMgr<MetadataSegment> {

    MetadataSegment findByName(String name);

    List<MetadataSegment> findAllInCollection(String collectionName);

    MetadataSegment findMasterSegment(String collectionName);

    void upsertStats(String segmentName, StatisticsContainer statisticsContainer);
}
