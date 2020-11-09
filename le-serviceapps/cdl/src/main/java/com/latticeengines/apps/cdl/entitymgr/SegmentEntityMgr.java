package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;

public interface SegmentEntityMgr extends BaseEntityMgr<MetadataSegment> {

    MetadataSegment findByName(String name);

    List<MetadataSegment> findByType(MetadataSegment.SegmentType type);

    List<MetadataSegment> findAllInCollection(String collectionName);

    MetadataSegment findMasterSegment(String collectionName);

    void upsertStats(String segmentName, StatisticsContainer statisticsContainer);

    MetadataSegment createSegment(MetadataSegment segment);

    MetadataSegment updateSegment(MetadataSegment segment, MetadataSegment existingSegment);

    MetadataSegment updateListSegment(MetadataSegment segment, MetadataSegment existingSegment);

    MetadataSegment updateSegmentWithoutActionAndAuditing(MetadataSegment segment, MetadataSegment existingSegment);

    void delete(MetadataSegment segment, Boolean ignoreDependencyCheck, Boolean hardDelete);

    void revertDelete(String segmentName);

    List<String> getAllDeletedSegments();

    MetadataSegment findByName(String name, boolean inflate);

    MetadataSegment createListSegment(MetadataSegment segment);

    MetadataSegment findByExternalInfo(String externalSystem, String externalSegmentId);

    MetadataSegment findByExternalInfo(MetadataSegment segment);
}
