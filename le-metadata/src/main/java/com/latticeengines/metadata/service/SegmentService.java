package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;

public interface SegmentService {

    MetadataSegment createOrUpdateSegment(String customerSpace, MetadataSegment segment);

    Boolean deleteSegmentByName(String customerSpace, String segmentName);

    List<MetadataSegment> getSegments(String customerSpace);

    List<MetadataSegment> getSegments(String customerSpace, String collectionName);

    MetadataSegment findByName(String customerSpace, String name);

    MetadataSegment findMaster(String customerSpace, String collectionName);

    StatisticsContainer getStats(String customerSpace, String segmentName);

    void deleteAllSegments(String customerSpace);
}
