package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;

public interface SegmentService {

    MetadataSegment createOrUpdateSegment(String customerSpace, MetadataSegment segment);

    Boolean deleteSegmentByName(String customerSpace, String segmentName);

    List<MetadataSegment> getSegments(String customerSpace);

    MetadataSegment findByName(String customerSpace, String name);
}
